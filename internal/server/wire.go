package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

const (
	// A complete revision request may contain both a one-million-code-point
	// description and content body. This ceiling admits both as maximum-width
	// UTF-8 plus framing while retaining an independent hard transport bound.
	maxLifecycleRequestBytes        = 13 << 20
	maxLifecycleStructuredJSONBytes = 64 << 10
	maxUUIDRepresentationCodePoints = 36
	maxLifecycleIdentityCodePoints  = 256
	maxLifecycleStageCodePoints     = 64
	maxLifecycleCodeCodePoints      = 128
	maxLifecycleMessageCodePoints   = 1024
	maxGenerationCandidates         = 101
	maxGenerationEvaluations        = 101
	maxGenerationDiagnostics        = storage.MaxGenerationDiagnostics
)

const (
	errorCodeInvalidRequest           = "invalid_request"
	errorCodeUnauthenticated          = "unauthenticated"
	errorCodeSkillNotFound            = "skill_not_found"
	errorCodeRevisionNotFound         = "revision_not_found"
	errorCodeRevisionPrivate          = "revision_private"
	errorCodeRevisionNotPublic        = "revision_not_public"
	errorCodeLatestRevisionConflict   = "latest_revision_conflict"
	errorCodeRevisionSequenceConflict = "revision_sequence_conflict"
	errorCodeGenerationNotFound       = "generation_not_found"
	errorCodeInvalidGenerationState   = "invalid_generation_state"
	errorCodePersistenceNotConfigured = "not_configured"
	errorCodeInternal                 = "internal_error"
)

// authContext contains identity established by the trusted gateway. Request
// bodies never supply creator attribution.
type authContext struct {
	Subject string
}

// authSubjectHeader carries the gateway-trusted JWT subject.
const authSubjectHeader = "x-paper-auth-subject"

func authContextFromRequest(r *http.Request) authContext {
	rawSubject := r.Header.Get(authSubjectHeader)
	subject := strings.TrimSpace(rawSubject)
	if !validBoundedIdentityText(rawSubject, maxLifecycleIdentityCodePoints) ||
		!validBoundedIdentityText(subject, maxLifecycleIdentityCodePoints) {
		return authContext{}
	}
	return authContext{Subject: subject}
}

func requireAuthContext(w http.ResponseWriter, r *http.Request) (authContext, bool) {
	ctx := authContextFromRequest(r)
	if ctx.Subject == "" {
		writeLifecycleError(w, http.StatusUnauthorized, errorCodeUnauthenticated, "Authentication is required.", "")
		return authContext{}, false
	}
	return ctx, true
}

func lifecycleOwner(w http.ResponseWriter, r *http.Request) (string, bool) {
	ctx, ok := requireAuthContext(w, r)
	return ctx.Subject, ok
}

func decodeLifecycleJSONBody(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxLifecycleRequestBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if !utf8.Valid(body) {
		return errors.New("request body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body contains more than one JSON value")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, `{"error":{"code":"internal_error","message":"The response could not be encoded."}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// lifecycleErrorResponse is the stable bounded error envelope shared by skill,
// revision, latest, and generation operations.
type lifecycleErrorResponse struct {
	Error lifecycleError `json:"error"`
}

type lifecycleError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	ResourceID string `json:"resourceId,omitempty"`
}

func writeLifecycleError(w http.ResponseWriter, status int, code, message, resourceID string, _ ...*int) {
	writeJSON(w, status, lifecycleErrorResponse{Error: lifecycleError{
		Code:       boundedLifecycleText(code, maxLifecycleCodeCodePoints),
		Message:    boundedLifecycleText(message, maxLifecycleMessageCodePoints),
		ResourceID: boundedLifecycleText(resourceID, maxLifecycleIdentityCodePoints),
	}})
}

// writeLifecycleStorageError is the one storage-to-HTTP lifecycle mapping.
// Inaccessible private resources intentionally share not-found responses.
func (s *Server) writeLifecycleStorageError(w http.ResponseWriter, operation string, err error) {
	var revisionNotPublic *storage.RevisionNotPublicError
	var explicitLatest *storage.RevisionIsExplicitLatestError
	switch {
	case errors.As(err, &revisionNotPublic):
		writeLifecycleError(w, http.StatusConflict, errorCodeRevisionNotPublic, "Only a public revision can be marked latest.", revisionNotPublic.RevisionID)
	case errors.Is(err, storage.ErrRevisionNotPublic):
		writeLifecycleError(w, http.StatusConflict, errorCodeRevisionNotPublic, "Only a public revision can be marked latest.", "")
	case errors.As(err, &explicitLatest):
		writeLifecycleError(w, http.StatusConflict, errorCodeLatestRevisionConflict, "Move or clear latest before making this revision private.", explicitLatest.RevisionID)
	case errors.Is(err, storage.ErrRevisionIsExplicitLatest):
		writeLifecycleError(w, http.StatusConflict, errorCodeLatestRevisionConflict, "Move or clear latest before making this revision private.", "")
	case errors.Is(err, storage.ErrSkillNotFound):
		writeLifecycleError(w, http.StatusNotFound, errorCodeSkillNotFound, "The requested skill was not found.", "")
	case errors.Is(err, storage.ErrRevisionNotFound), errors.Is(err, storage.ErrRevisionLineageInvalid):
		writeLifecycleError(w, http.StatusNotFound, errorCodeRevisionNotFound, "The requested revision was not found.", "")
	case errors.Is(err, storage.ErrRevisionConflict), errors.Is(err, storage.ErrSkillVersionConflict):
		writeLifecycleError(w, http.StatusConflict, errorCodeRevisionSequenceConflict, "The revision could not be appended because its identity conflicts.", "")
	case errors.Is(err, storage.ErrGenerationNotFound):
		writeLifecycleError(w, http.StatusNotFound, errorCodeGenerationNotFound, "The requested generation was not found.", "")
	case errors.Is(err, storage.ErrInvalidGenerationState):
		writeLifecycleError(w, http.StatusConflict, errorCodeInvalidGenerationState, "The generation cannot be changed from its current state.", "")
	default:
		s.logger.Error(operation, "error", err)
		writeLifecycleError(w, http.StatusInternalServerError, errorCodeInternal, "The request could not be completed.", "")
	}
}

func validateSelectedSessionIDs(sessionIDs []string) ([]string, string) {
	selected := make([]string, 0, len(sessionIDs))
	seen := make(map[string]struct{}, len(sessionIDs))
	for _, raw := range sessionIDs {
		sessionID := strings.TrimSpace(raw)
		if sessionID == "" {
			return nil, "selectedSessionIds must not contain empty values."
		}
		if !validBoundedIdentityText(raw, maxLifecycleIdentityCodePoints) ||
			!validBoundedIdentityText(sessionID, maxLifecycleIdentityCodePoints) {
			return nil, "selectedSessionIds contains an invalid or oversized value."
		}
		if _, exists := seen[sessionID]; exists {
			return nil, "selectedSessionIds must not contain duplicates."
		}
		seen[sessionID] = struct{}{}
		selected = append(selected, sessionID)
	}
	return selected, ""
}

func validBoundedText(value string, maxCodePoints int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxCodePoints {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\r' && character != '\t' {
			return false
		}
	}
	return true
}

// validBoundedIdentityText rejects every control character. Multiline text
// fields may contain the ordinary markdown whitespace accepted by
// validBoundedText, but identifiers, tags, and session references may not.
func validBoundedIdentityText(value string, maxCodePoints int) bool {
	if !validBoundedText(value, maxCodePoints) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validationMessage(err error) string {
	message := strings.TrimSpace(err.Error())
	if message == "" || !utf8.ValidString(message) {
		return "The request is invalid."
	}
	first, size := utf8.DecodeRuneInString(message)
	message = string(unicode.ToUpper(first)) + message[size:]
	if !strings.HasSuffix(message, ".") {
		message += "."
	}
	return boundedLifecycleText(message, maxLifecycleMessageCodePoints)
}

// canonicalUUID accepts bounded standard or raw-hex UUID representations and
// returns the lowercase hyphenated identity used at all storage boundaries.
// This keeps memory and Postgres behavior identical for noncanonical clients.
func canonicalUUID(value string) (string, error) {
	if !validBoundedIdentityText(value, maxUUIDRepresentationCodePoints) {
		return "", errors.New("invalid UUID representation")
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func nonNilWireStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339Nano)
	return &formatted
}

func boundedLifecycleText(value string, maxCodePoints int) string {
	if !utf8.ValidString(value) {
		value = strings.ToValidUTF8(value, "�")
	}
	if utf8.RuneCountInString(value) <= maxCodePoints {
		return value
	}
	return string([]rune(value)[:maxCodePoints])
}
