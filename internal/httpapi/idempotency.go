package httpapi

import (
	"fmt"
	"net/http"

	"example.com/solo-0021-metrology-assurance-suite/internal/assurance"
	"example.com/solo-0021-metrology-assurance-suite/internal/domain"
)

const (
	// IdempotencyKeyHeader carries a caller-chosen stable identifier for one
	// business submission. Requests without it keep the original semantics.
	IdempotencyKeyHeader = "Idempotency-Key"
	// IdempotencyReplayedHeader is set on responses served from the stored
	// result of the first successful submission, so callers can distinguish a
	// replay from a freshly executed action.
	IdempotencyReplayedHeader = "Idempotency-Replayed"

	maxIdempotencyKeyLength = 200
)

// retryIdentity resolves the stable retry identifier for a write request. The
// header is optional: when absent or blank the request runs without
// idempotency tracking exactly as before. The scope binds the key to the
// concrete route and path values, and the fingerprint pins the submitted
// content so a reused key with different input is rejected.
func retryIdentity(request *http.Request, input any) (assurance.RetryIdentity, error) {
	key := request.Header.Get(IdempotencyKeyHeader)
	if key == "" {
		return assurance.RetryIdentity{}, nil
	}
	if len(key) > maxIdempotencyKeyLength {
		return assurance.RetryIdentity{}, fmt.Errorf(
			"%w: %s must be at most %d characters",
			domain.ErrInvalidInput, IdempotencyKeyHeader, maxIdempotencyKeyLength,
		)
	}
	fingerprint, err := assurance.FingerprintRequest(input)
	if err != nil {
		return assurance.RetryIdentity{}, err
	}
	return assurance.RetryIdentity{
		Scope:       request.Method + " " + request.URL.Path,
		Key:         key,
		Fingerprint: fingerprint,
	}, nil
}

// writeSubmission emits either a fresh result or the replayed first result.
func writeSubmission[T any](writer http.ResponseWriter, outcome assurance.Submission[T]) {
	if outcome.Replayed {
		writer.Header().Set(IdempotencyReplayedHeader, "true")
	}
	writeJSON(writer, outcome.Status, outcome.Value)
}
