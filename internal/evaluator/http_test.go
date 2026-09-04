package evaluator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

func TestEvaluator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Evaluator Suite")
}

var _ = Describe("candidate evaluator HTTP adapter", func() {
	It("evaluator_http_adapter_round_trips_candidate_contract", func() {
		var received map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Method).To(Equal(http.MethodPost))
			Expect(r.URL.Path).To(Equal(evaluator.CandidateEvaluationsPath))
			Expect(r.Header.Get("x-paper-auth-subject")).To(Equal("owner-subject"))
			Expect(r.Header.Get("Content-Type")).To(Equal("application/json"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
			w.Header().Set("Content-Type", "application/json")
			_, err := w.Write([]byte(`{
				"ref":{"source":"skills-cassette","id":"generation-1/candidate-2"},
				"profile":"generation-candidate-v1",
				"profile_version":"1",
				"evaluator_version":"v0.4.0",
				"score":0.82,
				"decision":"revise",
				"criterion_results":[{"criterion_id":"safe-steps","weight":2,"passed":false,"rationale":"Add a check."}],
				"findings":[
					{"rule_id":"unsafe","severity":"critical","message":"Missing safety check."},
					{"rule_id":"clarity","severity":"warn","message":"Clarify the result."},
					{"rule_id":"note","severity":"info","message":"Good structure."}
				],
				"strengths":["Uses imperative steps."],
				"panel":{"judge_count":3,"agreement":0.91}
			}`))
			Expect(err).NotTo(HaveOccurred())
		}))
		defer server.Close()

		client := evaluator.NewHTTPClient(server.URL+evaluator.CandidateEvaluationsPath, server.Client())
		result, err := client.EvaluateCandidate(context.Background(), evaluator.CandidateEvaluationRequest{
			Ref: "generation-1/candidate-2", Name: "Candidate two",
			Candidate: evaluator.CandidateBundle{
				Slug: "candidate-two", Name: "Candidate Two", Description: "Use when testing the adapter.",
				Type: "workflow", Tags: []string{"testing"}, Content: "## Steps\n\n1. Verify it.",
				SourceSessionIDs: []string{"session-b"}, ParentID: "parent-skill",
			},
			Baseline: &evaluator.CandidateBundle{
				Slug: "baseline", Name: "Baseline", Description: "Use as the baseline.",
				Type: "workflow", Content: "## Steps\n\n1. Start.",
			},
			AuthorContext: "keep checks explicit", EvidenceSessionIDs: []string{"session-a", "session-b"},
			OwnerSubject: "owner-subject", Profile: evaluator.GenerationCandidateProfile,
			ProfileVersion: "1", Criteria: []evaluator.Criterion{{
				ID: "safe-steps", Kind: "content", Description: "The skill includes a safety check.", Weight: 2,
			}},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(received).To(HaveKeyWithValue("name", "Candidate two"))
		Expect(received).To(HaveKeyWithValue("profile", evaluator.GenerationCandidateProfile))
		Expect(received).To(HaveKeyWithValue("profile_version", "1"))
		Expect(received).To(HaveKeyWithValue("criteria", HaveLen(1)))
		Expect(received).To(HaveKeyWithValue("author_context", "keep checks explicit"))
		Expect(received).To(HaveKeyWithValue("evidence_session_ids", []any{"session-a", "session-b"}))
		ref := received["ref"].(map[string]any)
		Expect(ref).To(HaveKeyWithValue("source", "skills-cassette"))
		Expect(ref).To(HaveKeyWithValue("id", "generation-1/candidate-2"))
		candidate := received["candidate"].(map[string]any)
		Expect(candidate["skill_md"]).To(ContainSubstring(`name: "Candidate Two"`))
		Expect(candidate["skill_md"]).To(ContainSubstring("## Steps"))
		baseline := received["baseline"].(map[string]any)
		Expect(baseline["skill_md"]).To(ContainSubstring(`name: "Baseline"`))
		Expect(received).NotTo(HaveKey("owner_subject"))

		Expect(result.Profile).To(Equal(evaluator.GenerationCandidateProfile))
		Expect(result.ProfileVersion).To(Equal("1"))
		Expect(result.EvaluatorVersion).To(Equal("v0.4.0"))
		Expect(result.Score).NotTo(BeNil())
		Expect(*result.Score).To(Equal(0.82))
		Expect(result.Decision).To(Equal("revise"))
		Expect(result.CriticalFindingCount).To(Equal(1))
		Expect(result.WarningFindingCount).To(Equal(1))
		Expect(result.CriterionResults).To(MatchJSON(`[{"criterion_id":"safe-steps","weight":2,"passed":false,"rationale":"Add a check."}]`))
		Expect(result.Strengths).To(MatchJSON(`["Uses imperative steps."]`))
		Expect(result.Panel).To(MatchJSON(`{"judge_count":3,"agreement":0.91}`))

		secretServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "provider secret token must not escape", http.StatusServiceUnavailable)
		}))
		defer secretServer.Close()
		_, err = evaluator.NewHTTPClient(secretServer.URL, secretServer.Client()).EvaluateCandidate(
			context.Background(), evaluator.CandidateEvaluationRequest{
				Ref: "secret-test", Profile: evaluator.GenerationCandidateProfile,
				ProfileVersion: "1", Criteria: evaluator.GenerationCandidateCriteria().Criteria,
			})
		var callError *evaluator.CallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeTrue())
		Expect(callError.Code).To(Equal("evaluator_unavailable"))
		Expect(callError.Message).NotTo(ContainSubstring("secret token"))
		Expect(len(callError.Message)).To(BeNumerically("<=", 1024))

		invalidServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, writeErr := w.Write([]byte(`{"score":"not-a-number"}`))
			Expect(writeErr).NotTo(HaveOccurred())
		}))
		defer invalidServer.Close()
		_, err = evaluator.NewHTTPClient(invalidServer.URL, invalidServer.Client()).EvaluateCandidate(
			context.Background(), evaluator.CandidateEvaluationRequest{
				Ref: "invalid-test", Profile: evaluator.GenerationCandidateProfile,
				ProfileVersion: "1", Criteria: evaluator.GenerationCandidateCriteria().Criteria,
			})
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Retryable).To(BeFalse())
		Expect(callError.Code).To(Equal("evaluator_invalid_response"))
		Expect(strings.ToLower(callError.Message)).NotTo(ContainSubstring("not-a-number"))
	})

	It("admits two maximum legal JSON-escaped snapshots under one fixed transport ceiling", func() {
		escapedUnique := func(count, width int) []string {
			values := make([]string, count)
			alphabet := []byte{'<', '>', '&'}
			for index := range values {
				encoded := []byte(strings.Repeat("<", width))
				value := index
				for position := width - 1; value > 0; position-- {
					encoded[position] = alphabet[value%len(alphabet)]
					value /= len(alphabet)
				}
				values[index] = string(encoded)
			}
			return values
		}
		bundle := evaluator.CandidateBundle{
			Name:             strings.Repeat("<", storage.MaxRevisionNameCodePoints),
			Description:      strings.Repeat("<", storage.MaxRevisionDescriptionCodePoints),
			Type:             "workflow",
			Tags:             escapedUnique(storage.MaxRevisionTags, storage.MaxRevisionIdentityCodePoints),
			Content:          strings.Repeat("<", storage.MaxRevisionContentCodePoints),
			SourceSessionIDs: escapedUnique(storage.MaxRevisionSourceSessionIDs, storage.MaxRevisionIdentityCodePoints),
		}
		request := evaluator.CandidateEvaluationRequest{
			Ref: "maximum-snapshots", Name: "maximum snapshots", Candidate: bundle, Baseline: &bundle,
			Profile: evaluator.GenerationCandidateProfile, ProfileVersion: "1",
			Criteria: []evaluator.Criterion{{ID: "maximum", Kind: "content", Description: "Bounded.", Weight: 1}},
		}
		var receivedBytes int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			Expect(err).NotTo(HaveOccurred())
			receivedBytes = len(body)
			_, err = w.Write([]byte(`{
				"ref":{"source":"skills-cassette","id":"maximum-snapshots"},
				"profile":"generation-candidate-v1","profile_version":"1","evaluator_version":"v1",
				"score":1,"decision":"pass",
				"criterion_results":[{"criterion_id":"maximum","weight":1,"passed":true,"rationale":"bounded"}],
				"findings":[],"strengths":[]}`))
			Expect(err).NotTo(HaveOccurred())
		}))
		defer server.Close()
		client := evaluator.NewHTTPClient(server.URL, server.Client())
		_, err := client.EvaluateCandidate(context.Background(), request)
		Expect(err).NotTo(HaveOccurred())
		Expect(receivedBytes).To(BeNumerically(">", 2<<20), "the old transport ceiling rejected two legal snapshots")

		callsAtBoundary := receivedBytes
		request.AuthorContext = strings.Repeat("<", 2<<20)
		_, err = client.EvaluateCandidate(context.Background(), request)
		var callError *evaluator.CallError
		Expect(errors.As(err, &callError)).To(BeTrue())
		Expect(callError.Code).To(Equal("evaluator_invalid_request"))
		Expect(receivedBytes).To(Equal(callsAtBoundary), "an over-ceiling body must not reach transport")
	})

	It("bounds evaluator_version by Unicode code points and rejects controls", func() {
		request := evaluator.CandidateEvaluationRequest{
			Ref: "version", Profile: evaluator.GenerationCandidateProfile, ProfileVersion: "1",
			Criteria: []evaluator.Criterion{{ID: "version", Kind: "content", Description: "Versioned.", Weight: 1}},
		}
		evaluateVersion := func(version string) error {
			encodedVersion, err := json.Marshal(version)
			Expect(err).NotTo(HaveOccurred())
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, writeErr := fmt.Fprintf(w, `{
					"ref":{"source":"skills-cassette","id":"version"},
					"profile":"generation-candidate-v1","profile_version":"1",
					"evaluator_version":%s,"score":0.8,"decision":"pass",
					"criterion_results":[{"criterion_id":"version","weight":1,"passed":true,"rationale":"ok"}],
					"findings":[],"strengths":[]}`, encodedVersion)
				Expect(writeErr).NotTo(HaveOccurred())
			}))
			defer server.Close()
			_, err = evaluator.NewHTTPClient(server.URL, server.Client()).EvaluateCandidate(context.Background(), request)
			return err
		}

		Expect(evaluateVersion(strings.Repeat("界", 256))).To(Succeed())
		for _, invalid := range []string{strings.Repeat("界", 257), "version\ncontrol", " version"} {
			var callError *evaluator.CallError
			Expect(errors.As(evaluateVersion(invalid), &callError)).To(BeTrue())
			Expect(callError.Code).To(Equal("evaluator_invalid_response"))
			Expect(callError.Retryable).To(BeFalse())
		}
	})

	It("accepts the evaluator's echoed revision ref fields as bounded opaque strings", func() {
		request := evaluator.CandidateEvaluationRequest{
			Ref: "generation-9/candidate-1", Profile: evaluator.GenerationCandidateProfile, ProfileVersion: "1",
			Criteria: []evaluator.Criterion{{ID: "safe", Kind: "content", Description: "Remain safe.", Weight: 1}},
		}
		var received map[string]any
		evaluateWithRef := func(ref string) error {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
				_, writeErr := fmt.Fprintf(w, `{
					"ref":%s,
					"profile":"generation-candidate-v1","profile_version":"1","evaluator_version":"v1",
					"score":0.8,"decision":"pass",
					"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}],
					"findings":[],"strengths":[]}`, ref)
				Expect(writeErr).NotTo(HaveOccurred())
			}))
			defer server.Close()
			_, err := evaluator.NewHTTPClient(server.URL, server.Client()).EvaluateCandidate(context.Background(), request)
			return err
		}

		Expect(evaluateWithRef(`{"source":"skills-cassette","id":"generation-9/candidate-1","revision":"","revision_sha256":""}`)).To(Succeed(),
			"the evaluator echoes its whole ref record, including empty durable-revision fields")
		Expect(evaluateWithRef(`{"source":"skills-cassette","id":"generation-9/candidate-1","revision":"opaque-revision","revision_sha256":"` +
			strings.Repeat("a", 64) + `"}`)).To(Succeed())
		Expect(evaluateWithRef(`{"source":"skills-cassette","id":"generation-9/candidate-1"}`)).To(Succeed())
		requestRef := received["ref"].(map[string]any)
		Expect(requestRef).To(Equal(map[string]any{"source": "skills-cassette", "id": "generation-9/candidate-1"}),
			"candidate evaluation requests identify work by source and id only")

		for _, invalid := range []string{
			`{"source":"skills-cassette","id":"generation-9/candidate-1","revision":"` + strings.Repeat("界", 257) + `"}`,
			`{"source":"skills-cassette","id":"generation-9/candidate-1","revision_sha256":"sha\n256"}`,
			`{"source":"skills-cassette","id":"other","revision":"","revision_sha256":""}`,
			`{"source":"other","id":"generation-9/candidate-1","revision":"","revision_sha256":""}`,
			`{"source":"skills-cassette","id":"generation-9/candidate-1","unknown":""}`,
		} {
			var callError *evaluator.CallError
			Expect(errors.As(evaluateWithRef(invalid), &callError)).To(BeTrue(), invalid)
			Expect(callError.Code).To(Equal("evaluator_invalid_response"), invalid)
			Expect(callError.Retryable).To(BeFalse())
		}
	})

	It("strictly validates and canonicalizes evaluator-owned detail JSON", func() {
		request := evaluator.CandidateEvaluationRequest{
			Ref: "strict", Profile: evaluator.GenerationCandidateProfile, ProfileVersion: "1",
			Criteria: []evaluator.Criterion{{ID: "safe", Kind: "content", Description: "Remain safe.", Weight: 1}},
		}
		validPrefix := `{
			"ref":{"source":"skills-cassette","id":"strict"},
			"profile":"generation-candidate-v1","profile_version":"1","evaluator_version":"v1",
			"score":0.8,"decision":"pass",`
		validDetails := `"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}],"findings":[],"strengths":[]`
		for _, malformed := range []string{
			validPrefix + `"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok","extra":true}],"findings":[],"strengths":[]}`,
			validPrefix + `"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}],"findings":[{"severity":"info","message":"ok","extra":true}],"strengths":[]}`,
			validPrefix + `"criterion_results":[],"findings":[],"strengths":[{"message":"not a string"}]}`,
			validPrefix + `"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok","extra":true,"extra":false}],"findings":[],"strengths":[]}`,
			validPrefix + `"criterion_results":[{"criterion_id":"safe","criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}],"findings":[],"strengths":[]}`,
			validPrefix + `"criterion_results":[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}],"findings":[{"severity":"info","severity":"info","message":"ok"}],"strengths":[]}`,
			validPrefix + validDetails + `,"panel":{"judge":{"score":1,"score":1}}}`,
			validPrefix + validDetails + `,"score":0.8}`,
			validPrefix + validDetails + `,"unknown":true}`,
			strings.Replace(validPrefix, `"id":"strict"`, `"id":"strict","id":"strict"`, 1) + validDetails + `}`,
		} {
			body := malformed
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, writeErr := w.Write([]byte(body))
				Expect(writeErr).NotTo(HaveOccurred())
			}))
			_, err := evaluator.NewHTTPClient(server.URL, server.Client()).EvaluateCandidate(context.Background(), request)
			server.Close()
			var callError *evaluator.CallError
			Expect(errors.As(err, &callError)).To(BeTrue())
			Expect(callError.Code).To(Equal("evaluator_invalid_response"))
			Expect(callError.Retryable).To(BeFalse())
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write([]byte(validPrefix + `
				"criterion_results": [ { "criterion_id":"safe", "weight":1, "passed":true, "rationale":"ok" } ],
				"findings": [ { "severity":"WARNING", "message":"watch this" } ],
				"strengths": [ "clear" ], "panel":{"private":"diagnostics"}}`))
			Expect(err).NotTo(HaveOccurred())
		}))
		defer server.Close()
		result, err := evaluator.NewHTTPClient(server.URL, server.Client()).EvaluateCandidate(context.Background(), request)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(result.CriterionResults)).To(Equal(`[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok"}]`))
		Expect(string(result.Findings)).To(Equal(`[{"severity":"warning","message":"watch this"}]`))
		Expect(string(result.Strengths)).To(Equal(`["clear"]`))
		Expect(result.Panel).To(MatchJSON(`{"private":"diagnostics"}`),
			"the adapter may consume panel diagnostics; processor/storage strip them before persistence")
	})
})
