# Metrology Assurance Suite

Metrology Assurance Suite is a Go HTTP service for bounded laboratory
measurement assurance work. It registers devices, opens verification cases,
records measurements, handles failed tolerance decisions, and seals traceable
evidence records.

## Run

```bash
go run ./cmd/metrology-service --addr 127.0.0.1:18121
```

The service listens on `127.0.0.1:18121` by default.

## Build And Verify

```bash
go build ./...
go run ./cmd/workflowcheck --workflow register-device
go run ./cmd/workflowcheck --workflow open-verification-case
go run ./cmd/workflowcheck --workflow record-measurement
go run ./cmd/workflowcheck --workflow reopen-verification-case
go run ./cmd/workflowcheck --workflow seal-verification
go run ./cmd/workflowcheck --workflow idempotent-retry
```

The workflow runner starts a fresh in-process HTTP service and exercises the
public API. It is a production smoke command, not a unit test.

Tests are intentionally deferred for this initialization baseline. The later
engineering-task stage will add the unit and integration test suite and replace
or extend these smoke checks with red/green verification tests.

## API

- `GET /healthz`
- `POST /devices`
- `GET /devices/{id}`
- `POST /cases`
- `GET /cases/{id}`
- `POST /cases/{id}/measurements`
- `POST /cases/{id}/reopen`
- `POST /cases/{id}/seal`
- `GET /cases/{id}/events`

## Idempotent Retries

Every mutating route (`POST /devices`, `POST /cases`,
`POST /cases/{id}/measurements`, `POST /cases/{id}/reopen`,
`POST /cases/{id}/seal`) accepts an optional stable retry identifier in the
`Idempotency-Key` header. Use it when a network jitter or client timeout leaves
the outcome of a business submission unknown.

- The key identifies one business submission and is scoped to the route plus
  the concrete path (for example the case id). The same key on a different
  route or resource starts an independent submission.
- A retry with the same key and the same request content returns the first
  result that was already formed: same status code, same response body, no
  duplicate device/case/measurement/reopen/seal. Replies served from the
  stored first result carry `Idempotency-Replayed: true`.
- The submitted content is pinned by a canonical fingerprint. Reusing a key
  with different content is rejected stably with `409 idempotency_conflict`,
  so a mismatched retry can never be mistaken for the original submission.
- A request that fails before taking effect is not remembered; retrying the
  same key after such a failure runs the action again.
- Concurrent retries carrying the same key and content are serialized, so
  exactly one business fact is created and the other calls replay it.
- Requests without `Idempotency-Key` (or with a blank value) keep the original,
  non-idempotent behavior unchanged.

Keys must be at most 200 characters. Replaying returns the first result as it
was formed at that time, not a view of later state changes.

## Layout

- `cmd/metrology-service`: executable HTTP server.
- `cmd/workflowcheck`: bounded workflow verification commands.
- `internal/httpapi`: route and JSON boundary.
- `internal/assurance`: workflow orchestration.
- `internal/domain`: state rules and invariants.
- `internal/repository`: copy-on-write in-memory persistence.
- `internal/contracts`: API request and response models.
- `internal/bootstrap`: concrete dependency wiring.

## Inputs And Outputs

Inputs are JSON HTTP requests. Outputs are JSON response envelopes with either a
resource view or a stable error object:

```json
{
  "error": {
    "code": "state_conflict",
    "message": "case is not awaiting measurement"
  }
}
```

The service has no environment variables and no external dependencies.
