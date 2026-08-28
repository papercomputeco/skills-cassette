package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// rawGET issues a request and returns the raw body plus status — for the
// byte-identity assertions where a decoded map would hide differences.
func rawGET(srv *server.Server, path string) (string, int) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recorder, req)
	return recorder.Body.String(), recorder.Code
}

// externalFilterStore wraps another Store so the unit specs can arm (or
// refuse) the startup probe and observe exactly what list options the handler
// threads through. Delegated lists strip the external filters first — the
// in-memory store deliberately refuses them.
type externalFilterStore struct {
	storage.Store
	probeErr error
	// probeFn, when set, decides each view's probe outcome (and sees the
	// probe context), overriding probeErr.
	probeFn  func(ctx context.Context, view string) error
	listErr  error
	countErr error
	lastOpts *storage.SkillListOpts
	// lastCountOpts records what the handler threads into the totals query.
	lastCountOpts *storage.SkillCountOpts
}

func (s *externalFilterStore) ProbeExternalView(ctx context.Context, view string) error {
	if s.probeFn != nil {
		return s.probeFn(ctx, view)
	}
	return s.probeErr
}

func (s *externalFilterStore) ListSkills(ctx context.Context, opts storage.SkillListOpts) ([]storage.SkillRecord, error) {
	captured := opts
	s.lastOpts = &captured
	if s.listErr != nil {
		return nil, s.listErr
	}
	stripped := opts
	stripped.External = nil
	return s.Store.ListSkills(ctx, stripped)
}

func (s *externalFilterStore) CountSkills(ctx context.Context, opts storage.SkillCountOpts) (storage.SkillCounts, error) {
	captured := opts
	s.lastCountOpts = &captured
	if s.countErr != nil {
		return storage.SkillCounts{}, s.countErr
	}
	stripped := opts
	stripped.External = nil
	return s.Store.CountSkills(ctx, stripped)
}

var _ = Describe("external attachment-view filters", func() {
	// The param name and view name below are deployment-supplied VALUES from
	// test configuration — nothing here is compiled into the cassette.
	deploymentConfig := `[{"param":"label","view":"attach_fixture.attachments","type_value":"skill","normalize":["trim","nfc","casefold"]}]`

	It("wires an external attachment-view filter from deployment config", func() {
		filters, err := server.ParseExternalFilters(deploymentConfig)
		Expect(err).NotTo(HaveOccurred())

		store := &externalFilterStore{Store: storage.NewMemoryStore()}
		seedSkill(store.Store.(*storage.MemoryStore), "a-skill")
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		// The configured param is repeatable and normalized per the declared
		// verbs before it reaches storage as one EXISTS-backed filter.
		_, status := doJSON(srv, http.MethodGet, "/api/skills?label=%20BUG%20&label=Stra%C3%9Fe", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(store.lastOpts).NotTo(BeNil())
		Expect(store.lastOpts.External).To(HaveLen(1))
		Expect(store.lastOpts.External[0].View).To(Equal("attach_fixture.attachments"))
		Expect(store.lastOpts.External[0].TypeValue).To(Equal("skill"))
		Expect(store.lastOpts.External[0].Values).To(Equal([]string{"bug", "strasse"}))

		// The per-tab totals are computed over the same filtered set: the
		// count query receives the identical armed filters as the page query.
		Expect(store.lastCountOpts).NotTo(BeNil())
		Expect(store.lastCountOpts.External).To(Equal(store.lastOpts.External))

		// Unconfigured: the capability is off with zero behavioral change —
		// the would-be param is never parsed and never reaches storage.
		off := &externalFilterStore{Store: storage.NewMemoryStore()}
		seedSkill(off.Store.(*storage.MemoryStore), "a-skill")
		offSrv := server.New(server.Config{}, off, nil, nil)
		withParam, status := rawGET(offSrv, "/api/skills?label=anything")
		Expect(status).To(Equal(http.StatusOK))
		without, _ := rawGET(offSrv, "/api/skills")
		Expect(withParam).To(Equal(without), "an unconfigured filter param must be byte-identical to its absence")
		Expect(off.lastOpts.External).To(BeEmpty())

		// Probe refused at startup: configured but not readable — same
		// fail-open behavior, the param is ignored.
		refused := &externalFilterStore{Store: storage.NewMemoryStore(), probeErr: fmt.Errorf("no such relation")}
		refusedSrv := server.New(server.Config{Filters: filters}, refused, nil, nil)
		_, status = rawGET(refusedSrv, "/api/skills?label=anything")
		Expect(status).To(Equal(http.StatusOK))
		Expect(refused.lastOpts.External).To(BeEmpty())
	})

	It("answers 503 when an armed filter cannot be evaluated for the totals", func() {
		// The page query succeeds but the totals query hits the broken view:
		// the same missing-relation convention applies — never totals that
		// silently ignore the filter the page honored.
		filters, err := server.ParseExternalFilters(deploymentConfig)
		Expect(err).NotTo(HaveOccurred())
		store := &externalFilterStore{
			Store:    storage.NewMemoryStore(),
			countErr: fmt.Errorf("count skills: %w", storage.ErrExternalViewUnavailable),
		}
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		body, status := doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
		Expect(status).To(Equal(http.StatusServiceUnavailable))
		Expect(body).To(HaveKey("error"))
		Expect(body).NotTo(HaveKey("items"))
	})

	It("probes each configured view under its own deadline", func() {
		DeferCleanup(server.StubExternalFilterProbeTimeout(50 * time.Millisecond))

		// Two configured filters; the first view's probe hangs past its own
		// deadline. The second filter's readable view must still be probed
		// with a live context and armed.
		twoFilters := `[{"param":"label","view":"attach_fixture.hanging","type_value":"skill"},
			{"param":"stage","view":"attach_fixture.healthy","type_value":"skill"}]`
		filters, err := server.ParseExternalFilters(twoFilters)
		Expect(err).NotTo(HaveOccurred())

		store := &externalFilterStore{Store: storage.NewMemoryStore()}
		seedSkill(store.Store.(*storage.MemoryStore), "a-skill")
		store.probeFn = func(ctx context.Context, view string) error {
			if view == "attach_fixture.hanging" {
				// Consume this probe's entire deadline before failing.
				<-ctx.Done()
				return ctx.Err()
			}
			// Report whatever budget the arming loop granted this probe: a
			// context already spent by the earlier hang means unarmed.
			return ctx.Err()
		}
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		// The filter behind the hanging probe stays unarmed...
		_, status := doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(store.lastOpts.External).To(BeEmpty())

		// ...while the later readable view is armed and filters requests.
		_, status = doJSON(srv, http.MethodGet, "/api/skills?stage=x", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(store.lastOpts.External).To(HaveLen(1))
		Expect(store.lastOpts.External[0].View).To(Equal("attach_fixture.healthy"))
	})

	It("answers 503 rather than unfiltered results when an armed filter cannot be evaluated", func() {
		filters, err := server.ParseExternalFilters(deploymentConfig)
		Expect(err).NotTo(HaveOccurred())
		store := &externalFilterStore{
			Store:   storage.NewMemoryStore(),
			listErr: fmt.Errorf("list skills: %w", storage.ErrExternalViewUnavailable),
		}
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		body, status := doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
		Expect(status).To(Equal(http.StatusServiceUnavailable))
		Expect(body).To(HaveKey("error"))
		Expect(body).NotTo(HaveKey("items"))
	})
})

// The Postgres specs exercise the whole path — startup probe, EXISTS
// rendering, pagination composition, and the loud broken-after-probe
// degradation — against a fixture view the TEST creates. The fixture matches
// the canonical attachment shape (primitive_type, primitive_id, value) the
// cassette documents for this capability; no external product is installed
// or referenced.
var _ = Describe("external attachment-view filters (Postgres)", func() {
	var (
		ctx       context.Context
		pool      *pgxpool.Pool
		store     *storage.PostgresStore
		schema    string
		fixture   string
		skillA    string
		skillB    string
		skillC    string
		newServer func(configJSON string) *server.Server
	)

	BeforeEach(func() {
		dsn := os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			dsn = os.Getenv("TEST_DATABASE_URL")
		}
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx = context.Background()

		var err error
		pool, err = pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		suffix := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
		schema = "skills_ext_" + suffix
		fixture = "attach_fixture_" + suffix

		store, err = storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())

		// Fixture: a backing table plus a view of the canonical attachment
		// shape. The column DDL matches core's published-view fixture
		// (testpub.attachments) — the two fixtures pin one contract shape.
		for _, statement := range []string{
			fmt.Sprintf(`CREATE SCHEMA %q`, fixture),
			fmt.Sprintf(`CREATE TABLE %q.rows (
				primitive_type text NOT NULL,
				primitive_id   text NOT NULL,
				value          text NOT NULL
			)`, fixture),
			fmt.Sprintf(`CREATE VIEW %q.attachments AS
				SELECT primitive_type, primitive_id, value FROM %q.rows`, fixture, fixture),
		} {
			_, err := pool.Exec(ctx, statement)
			Expect(err).NotTo(HaveOccurred())
		}

		now := time.Now().UTC().Truncate(time.Microsecond)
		skillA, skillB, skillC = uuid.NewString(), uuid.NewString(), uuid.NewString()
		for i, id := range []string{skillA, skillB, skillC} {
			_, err := store.UpsertSkill(ctx, storage.SkillRecord{
				ID: id, Slug: fmt.Sprintf("skill-%d", i), Name: fmt.Sprintf("Skill %d", i),
				Type: "workflow", Version: "0.1.0", Visibility: "private",
				CreatedAt: now.Add(time.Duration(i) * time.Second),
				UpdatedAt: now.Add(time.Duration(i) * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
		}

		// Attachment rows are fixture DATA: values arrive pre-folded, the way
		// the view contract defines them.
		for _, row := range [][3]string{
			{"skill", skillA, "alpha"},
			{"skill", skillA, "beta"},
			{"skill", skillB, "alpha"},
			{"other_type", skillC, "alpha"},
		} {
			_, err := pool.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %q.rows (primitive_type, primitive_id, value) VALUES ($1, $2, $3)`, fixture),
				row[0], row[1], row[2])
			Expect(err).NotTo(HaveOccurred())
		}

		newServer = func(configJSON string) *server.Server {
			var filters []server.ExternalFilter
			if configJSON != "" {
				var err error
				filters, err = server.ParseExternalFilters(configJSON)
				Expect(err).NotTo(HaveOccurred())
			}
			return server.New(server.Config{Filters: filters}, store, nil, nil)
		}
	})

	AfterEach(func() {
		if pool == nil {
			return
		}
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, fixture))
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
		store.Close()
		pool.Close()
		pool = nil
	})

	config := func(view string) string {
		return `[{"param":"label","view":"` + view + `","type_value":"skill","normalize":["casefold"]}]`
	}

	ids := func(body map[string]any) []string {
		items, _ := body["items"].([]any)
		out := make([]string, 0, len(items))
		for _, item := range items {
			id, _ := item.(map[string]any)["id"].(string)
			out = append(out, id)
		}
		return out
	}

	It("filters skills by label with AND and casefold when the view is available", func() {
		srv := newServer(config(fixture + ".attachments"))

		// One value, casefolded before binding; only the configured
		// type_value's rows count (skillC matches under another type).
		body, status := doJSON(srv, http.MethodGet, "/api/skills?label=ALPHA", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(ids(body)).To(ConsistOf(skillA, skillB))

		// Repeat is AND: only the skill carrying every value survives.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=Alpha&label=BETA", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(ids(body)).To(Equal([]string{skillA}))

		// Unmatchable values are an honest empty page, never an error.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=no_such_value", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(ids(body)).To(BeEmpty())
		Expect(body).NotTo(HaveKey("next_cursor"))

		// The filter composes with keyset pagination: page through the two
		// matches one row at a time.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=alpha&limit=1", "", "")
		Expect(status).To(Equal(http.StatusOK))
		firstPage := ids(body)
		Expect(firstPage).To(HaveLen(1))
		cursor, _ := body["next_cursor"].(string)
		Expect(cursor).NotTo(BeEmpty())
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=alpha&limit=1&cursor="+cursor, "", "")
		Expect(status).To(Equal(http.StatusOK))
		secondPage := ids(body)
		Expect(secondPage).To(HaveLen(1))
		Expect(append(firstPage, secondPage...)).To(ConsistOf(skillA, skillB))
	})

	It("counts the filtered set the page came from, not the whole table", func() {
		srv := newServer(config(fixture + ".attachments"))
		counts := func(body map[string]any) map[string]any {
			c, _ := body["counts"].(map[string]any)
			return c
		}

		// No filter param: the totals span every skill, unchanged.
		body, status := doJSON(srv, http.MethodGet, "/api/skills", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(counts(body)).To(HaveKeyWithValue("all", float64(3)))

		// Filtered: the tab totals must describe the same set as the page.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=ALPHA", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(ids(body)).To(ConsistOf(skillA, skillB))
		Expect(counts(body)).To(HaveKeyWithValue("all", float64(2)))

		// AND-narrowed values narrow the totals identically.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=alpha&label=beta", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(ids(body)).To(Equal([]string{skillA}))
		Expect(counts(body)).To(HaveKeyWithValue("all", float64(1)))

		// An unmatchable value is an honest zero, matching its empty page.
		body, status = doJSON(srv, http.MethodGet, "/api/skills?label=no_such_value", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(counts(body)).To(HaveKeyWithValue("all", float64(0)))
	})

	It("ignores the configured filter param when its view is absent at probe", func() {
		// Configured, but the view does not exist: the startup probe fails,
		// the capability stays off, and the param is byte-identically ignored.
		srv := newServer(config(fixture + ".no_such_view"))
		withParam, status := rawGET(srv, "/api/skills?label=alpha")
		Expect(status).To(Equal(http.StatusOK))
		without, _ := rawGET(srv, "/api/skills")
		Expect(withParam).To(Equal(without))

		// Unconfigured behaves the same way.
		unconfigured := newServer("")
		withParam, status = rawGET(unconfigured, "/api/skills?label=alpha")
		Expect(status).To(Equal(http.StatusOK))
		without, _ = rawGET(unconfigured, "/api/skills")
		Expect(withParam).To(Equal(without))
	})

	It("errors instead of silently unfiltering when the view breaks after probe", func() {
		srv := newServer(config(fixture + ".attachments"))

		// Armed and working first — then the view disappears mid-flight.
		_, status := doJSON(srv, http.MethodGet, "/api/skills?label=alpha", "", "")
		Expect(status).To(Equal(http.StatusOK))

		_, err := pool.Exec(ctx, fmt.Sprintf(`DROP VIEW %q.attachments`, fixture))
		Expect(err).NotTo(HaveOccurred())

		body, status := doJSON(srv, http.MethodGet, "/api/skills?label=alpha", "", "")
		Expect(status).To(Equal(http.StatusServiceUnavailable),
			"a broken armed filter must fail loudly, per the missing-relation convention")
		Expect(body).To(HaveKey("error"))
		Expect(body).NotTo(HaveKey("items"), "no skill rows may leak through a broken filter")

		// The unfiltered list still serves — only the filtered request fails.
		_, status = doJSON(srv, http.MethodGet, "/api/skills", "", "")
		Expect(status).To(Equal(http.StatusOK))

		// The totals query classifies the breakage the same way: the typed
		// error, never counts computed as if the filter were not configured.
		_, err = store.CountSkills(ctx, storage.SkillCountOpts{
			External: []storage.ExternalAttachmentFilter{{
				View: fixture + ".attachments", TypeValue: "skill", Values: []string{"alpha"},
			}},
		})
		Expect(err).To(MatchError(storage.ErrExternalViewUnavailable))
	})

	It("arms a filter whose view becomes readable only after startup", func() {
		DeferCleanup(server.StubExternalFilterReprobeInterval(20 * time.Millisecond))

		// The view does not exist yet when the server boots — the shape of
		// a deployment whose SELECT grant or view lands moments after the
		// pod starts. The param is ignored, exactly as before.
		srv := newServer(config(fixture + ".late_attachments"))
		withParam, status := rawGET(srv, "/api/skills?label=alpha")
		Expect(status).To(Equal(http.StatusOK))
		without, _ := rawGET(srv, "/api/skills")
		Expect(withParam).To(Equal(without))

		DeferCleanup(serveInBackground(srv))

		// The view materializes; the background loop arms the filter and
		// requests start filtering without a restart.
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE VIEW %q.late_attachments AS
			SELECT primitive_type, primitive_id, value FROM %q.rows`, fixture, fixture))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() []string {
			body, status := doJSON(srv, http.MethodGet, "/api/skills?label=alpha", "", "")
			if status != http.StatusOK {
				return nil
			}
			return ids(body)
		}, 2*time.Second).Should(ConsistOf(skillA, skillB))
	})
})

// reprobeStore reads external views for the re-probe specs. Probe outcomes
// are controlled per view and every probe attempt is counted, so the specs
// can observe exactly when the background loop fires and against which
// views. All state is mutex-guarded: probes arrive from the server's
// background loop while the specs read counts and issue requests.
type reprobeStore struct {
	storage.Store
	mu       sync.Mutex
	readable map[string]bool
	probes   map[string]int
	// hangProbes makes every probe consume its whole context before
	// failing, for the shutdown-under-a-hanging-probe spec.
	hangProbes   bool
	lastExternal []storage.ExternalAttachmentFilter
	// sawExternal sticks once any list carried an armed external filter —
	// an arming signal that concurrent unfiltered lists cannot overwrite.
	sawExternal bool
}

func newReprobeStore() *reprobeStore {
	return &reprobeStore{
		Store:    storage.NewMemoryStore(),
		readable: map[string]bool{},
		probes:   map[string]int{},
	}
}

func (s *reprobeStore) setReadable(view string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readable[view] = ok
}

func (s *reprobeStore) setHangProbes(hang bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hangProbes = hang
}

func (s *reprobeStore) probeCount(view string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes[view]
}

func (s *reprobeStore) listedWithExternal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawExternal
}

// external returns the external filters the most recent list threaded into
// storage — the observable proof of which filters were armed at that moment.
func (s *reprobeStore) external() []storage.ExternalAttachmentFilter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastExternal
}

func (s *reprobeStore) ProbeExternalView(ctx context.Context, view string) error {
	s.mu.Lock()
	s.probes[view]++
	readable := s.readable[view]
	hang := s.hangProbes
	s.mu.Unlock()
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if !readable {
		return fmt.Errorf("view %s is not readable", view)
	}
	return nil
}

func (s *reprobeStore) ListSkills(ctx context.Context, opts storage.SkillListOpts) ([]storage.SkillRecord, error) {
	s.mu.Lock()
	s.lastExternal = opts.External
	if len(opts.External) > 0 {
		s.sawExternal = true
	}
	s.mu.Unlock()
	stripped := opts
	stripped.External = nil
	return s.Store.ListSkills(ctx, stripped)
}

func (s *reprobeStore) CountSkills(ctx context.Context, opts storage.SkillCountOpts) (storage.SkillCounts, error) {
	stripped := opts
	stripped.External = nil
	return s.Store.CountSkills(ctx, stripped)
}

// serveInBackground runs srv.Serve on a loopback listener so the background
// re-probe loop is live, and returns a stop func that cancels the server and
// asserts it exits cleanly.
func serveInBackground(srv *server.Server) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, listener) }()
	return func() {
		cancel()
		Eventually(done, 3*time.Second).Should(Receive(BeNil()),
			"the server (and its re-probe loop) must shut down cleanly")
	}
}

var _ = Describe("external filter re-probing", func() {
	// As everywhere in these specs, the param and view names are
	// deployment-supplied VALUES from test configuration.
	const view = "attach_fixture.attachments"
	config := `[{"param":"label","view":"` + view + `","type_value":"skill"}]`

	It("arms an unreadable-at-boot filter once its view becomes readable", func() {
		DeferCleanup(server.StubExternalFilterReprobeInterval(20 * time.Millisecond))

		filters, err := server.ParseExternalFilters(config)
		Expect(err).NotTo(HaveOccurred())
		store := newReprobeStore()
		seedSkill(store.Store.(*storage.MemoryStore), "a-skill")
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		// Unreadable at boot: the param is ignored, exactly as before.
		_, status := doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
		Expect(status).To(Equal(http.StatusOK))
		Expect(store.external()).To(BeEmpty())

		DeferCleanup(serveInBackground(srv))

		// The view becomes readable shortly after boot — the deployment's
		// grant converging — and the background loop arms the filter
		// without a restart.
		store.setReadable(view, true)
		Eventually(func() []storage.ExternalAttachmentFilter {
			_, _ = doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
			return store.external()
		}, time.Second).Should(HaveLen(1))
		Expect(store.external()[0].View).To(Equal(view))
	})

	It("re-probes only unarmed filters and stops once everything is armed", func() {
		DeferCleanup(server.StubExternalFilterReprobeInterval(20 * time.Millisecond))

		const readyView = "attach_fixture.ready"
		const lateView = "attach_fixture.late"
		filters, err := server.ParseExternalFilters(
			`[{"param":"label","view":"` + readyView + `","type_value":"skill"},
			  {"param":"stage","view":"` + lateView + `","type_value":"skill"}]`)
		Expect(err).NotTo(HaveOccurred())

		store := newReprobeStore()
		store.setReadable(readyView, true)
		seedSkill(store.Store.(*storage.MemoryStore), "a-skill")
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)
		Expect(store.probeCount(readyView)).To(Equal(1), "startup probes each view once")

		DeferCleanup(serveInBackground(srv))

		// The unarmed view is retried on the interval; the armed one never
		// sees another probe.
		Eventually(func() int { return store.probeCount(lateView) }, time.Second).
			Should(BeNumerically(">=", 3))
		Expect(store.probeCount(readyView)).To(Equal(1))

		// Once the late view becomes readable it arms...
		store.setReadable(lateView, true)
		Eventually(func() []storage.ExternalAttachmentFilter {
			_, _ = doJSON(srv, http.MethodGet, "/api/skills?stage=x", "", "")
			return store.external()
		}, time.Second).Should(HaveLen(1))
		Expect(store.external()[0].View).To(Equal(lateView))

		// ...and with nothing left unarmed the loop stops probing at all.
		settled := store.probeCount(lateView)
		Consistently(func() int { return store.probeCount(lateView) }, 200*time.Millisecond).
			Should(Equal(settled))
		Expect(store.probeCount(readyView)).To(Equal(1))
	})

	It("serves list reads race-free while the loop arms a filter", func() {
		DeferCleanup(server.StubExternalFilterReprobeInterval(time.Millisecond))

		filters, err := server.ParseExternalFilters(config)
		Expect(err).NotTo(HaveOccurred())
		store := newReprobeStore()
		seedSkill(store.Store.(*storage.MemoryStore), "a-skill")
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)
		DeferCleanup(serveInBackground(srv))

		// Hammer the list from several goroutines while the background loop
		// arms the filter mid-flight. The race detector is the assertion.
		stopHammer := make(chan struct{})
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				for {
					select {
					case <-stopHammer:
						return
					default:
						_, status := rawGET(srv, "/api/skills")
						Expect(status).To(Equal(http.StatusOK))
					}
				}
			}()
		}
		store.setReadable(view, true)
		// The sticky signal, not lastExternal: the hammering unfiltered
		// lists legitimately record an empty filter set right after the
		// filtered one lands.
		Eventually(func() bool {
			_, _ = doJSON(srv, http.MethodGet, "/api/skills?label=x", "", "")
			return store.listedWithExternal()
		}, time.Second).Should(BeTrue())
		close(stopHammer)
		wg.Wait()
	})

	It("shuts down cleanly while an unarmed filter is mid-probe", func() {
		DeferCleanup(server.StubExternalFilterReprobeInterval(20 * time.Millisecond))

		filters, err := server.ParseExternalFilters(config)
		Expect(err).NotTo(HaveOccurred())
		store := newReprobeStore()
		srv := server.New(server.Config{Filters: filters}, store, nil, nil)

		// Every re-probe from here on hangs for its whole deadline (5s by
		// default). Shutdown must not wait it out: canceling the server
		// must cancel the in-flight probe too.
		store.setHangProbes(true)
		stop := serveInBackground(srv)
		Eventually(func() int { return store.probeCount(view) }, time.Second).
			Should(BeNumerically(">=", 2), "a re-probe attempt is in flight")
		stop()
	})
})
