package generation_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/generation"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// admissionWorkerRuntime keeps durable time under the test's control while
// leaving the worker's own polling timers real and short.
func admissionWorkerRuntime(clock *fakeStoreClock) generation.WorkerRuntime {
	return generation.WorkerRuntime{
		Clock:       clock.Now,
		RandomFloat: func() float64 { return 0.5 },
		NewTimer: func(time.Duration) generation.WorkerTimer {
			return &workerTimer{timer: time.NewTimer(time.Millisecond)}
		},
	}
}

func admissionWorkerConfig(closed bool) generation.WorkerConfig {
	config := fastWorkerConfig()
	config.WorkerConcurrency = 1
	config.LeaseDuration = time.Hour
	config.HeartbeatInterval = 30 * time.Minute
	config.ProcessingTimeout = time.Hour
	config.MaxAttempts = 4
	config.AdmissionClosed = closed
	return config
}

// seedAdmissionWorkerGeneration commits one queued row against a skill, the
// shape of work admitted before the deployment closed admission.
func seedAdmissionWorkerGeneration(
	store interface {
		storage.SkillIdentityStore
		storage.GenerationStore
	},
	slug string,
	createdAt time.Time,
) (string, string) {
	skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
		ID: uuid.NewString(), Slug: slug, CreatorSubject: workerOwner, CreatedAt: createdAt,
	})
	Expect(err).NotTo(HaveOccurred())
	generationID := uuid.NewString()
	createQueueGeneration(store, skillRecord.ID, generationID, createdAt)
	return skillRecord.ID, generationID
}

var _ = Describe("generation worker admission", func() {
	It("claims and completes already-enqueued work while admission is closed", func() {
		store := storage.NewMemoryStore()
		DeferCleanup(store.Close)
		_, generationID := seedAdmissionWorkerGeneration(store, "admission-drain", time.Now().UTC().Add(-time.Minute))

		var claims atomic.Int32
		processor := claimProcessorFunc(func(ctx context.Context, claimedID, claimToken string) (*generation.ProcessResult, error) {
			claims.Add(1)
			Expect(claimedID).To(Equal(generationID))
			state, err := store.GetGenerationByID(ctx, workerOwner, claimedID)
			Expect(err).NotTo(HaveOccurred())
			claimed := state.Generation
			claimed.ClaimToken = claimToken
			completeClaimedGeneration(store, &claimed)
			return &generation.ProcessResult{}, nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorker(store, processor, admissionWorkerConfig(true), nil, nil).Run(ctx)
		}()
		waitUntil(2*time.Second, func() bool {
			state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusCompleted
		})
		cancel()
		Eventually(done).WithTimeout(2 * time.Second).Should(Receive(Succeed()))

		state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.Status).To(Equal(storage.GenerationStatusCompleted))
		Expect(state.Generation.ResultRevisionID).NotTo(BeEmpty(),
			"closing admission must not discard work the deployment already accepted")
		Expect(claims.Load()).To(Equal(int32(1)))
	})

	It("continues a lease-expired row while admission is closed", func() {
		clock := &fakeStoreClock{now: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)}
		store := storage.NewMemoryStoreWithClock(clock.Now)
		DeferCleanup(store.Close)
		_, generationID := seedAdmissionWorkerGeneration(store, "admission-recovery", clock.Now().Add(-time.Minute))

		By("losing a worker mid-attempt: the claim stands until its lease expires")
		abandoned, err := store.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{
			WorkerID: "worker-that-died", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(abandoned.ID).To(Equal(generationID))
		Expect(abandoned.AttemptCount).To(Equal(1))
		clock.Advance(2 * time.Minute)

		var recovered atomic.Int32
		processor := claimProcessorFunc(func(_ context.Context, claimedID, _ string) (*generation.ProcessResult, error) {
			Expect(claimedID).To(Equal(generationID))
			recovered.Add(1)
			return &generation.ProcessResult{}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorkerWithRuntime(store, processor, admissionWorkerConfig(true),
				nil, nil, admissionWorkerRuntime(clock)).Run(ctx)
		}()
		waitUntil(2*time.Second, func() bool { return recovered.Load() > 0 })
		cancel()
		Eventually(done).WithTimeout(2 * time.Second).Should(Receive(Succeed()))

		state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.AttemptCount).To(Equal(2),
			"crash recovery continues an admitted attempt; it does not admit new work")
	})

	It("does not retry a failed attempt while admission is closed", func() {
		clock := &fakeStoreClock{now: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)}
		memory := storage.NewMemoryStoreWithClock(clock.Now)
		DeferCleanup(memory.Close)
		store := &recordingSchedulingStore{MemoryStore: memory}
		store.advance = func(delay time.Duration) { clock.Advance(delay + time.Nanosecond) }
		_, generationID := seedAdmissionWorkerGeneration(store, "admission-no-retry", clock.Now().Add(-time.Minute))

		var attempts atomic.Int32
		processor := claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
			attempts.Add(1)
			return nil, &generation.ProcessError{
				Code: "transient-provider-error", Message: "retry me", Retryable: true,
			}
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorkerWithRuntime(store, processor, admissionWorkerConfig(true),
				nil, nil, admissionWorkerRuntime(clock)).Run(ctx)
		}()
		waitUntil(2*time.Second, func() bool {
			state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Eventually(done).WithTimeout(2 * time.Second).Should(Receive(Succeed()))

		requeues, failures := store.schedulingCalls()
		Expect(requeues).To(BeEmpty(), "a retry is new admission and must not be scheduled")
		Expect(failures).To(HaveLen(1))
		Expect(failures[0].Failure).To(Equal(storage.GenerationFailure{
			Code: "attempts_exhausted", Message: "Generation retry attempts were exhausted.",
		}), "the terminal shape stays the one callers already handle")
		Expect(attempts.Load()).To(Equal(int32(1)),
			"the ladder the retry backoff would have run is exactly what no drain deadline covers")

		state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.Status).To(Equal(storage.GenerationStatusFailed))
		Expect(state.Generation.AttemptCount).To(Equal(1))
		Expect(state.Generation.ErrorCode).To(Equal("attempts_exhausted"))
	})

	It("still retries a failed attempt while admission is open", func() {
		clock := &fakeStoreClock{now: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)}
		memory := storage.NewMemoryStoreWithClock(clock.Now)
		DeferCleanup(memory.Close)
		store := &recordingSchedulingStore{MemoryStore: memory}
		store.advance = func(delay time.Duration) { clock.Advance(delay + time.Nanosecond) }
		_, generationID := seedAdmissionWorkerGeneration(store, "admission-retry", clock.Now().Add(-time.Minute))

		var attempts atomic.Int32
		processor := claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
			attempts.Add(1)
			return nil, &generation.ProcessError{
				Code: "transient-provider-error", Message: "retry me", Retryable: true,
			}
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorkerWithRuntime(store, processor, admissionWorkerConfig(false),
				nil, nil, admissionWorkerRuntime(clock)).Run(ctx)
		}()
		waitUntil(3*time.Second, func() bool {
			state, err := store.GetGenerationByID(context.Background(), workerOwner, generationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Eventually(done).WithTimeout(2 * time.Second).Should(Receive(Succeed()))

		requeues, failures := store.schedulingCalls()
		Expect(requeues).To(HaveLen(3), "the open deployment runs the full durable retry ladder")
		for _, requeue := range requeues {
			Expect(requeue.Failure.Code).To(Equal("generation_retrying"))
		}
		Expect(failures).To(HaveLen(1))
		Expect(attempts.Load()).To(Equal(int32(4)))
	})
})
