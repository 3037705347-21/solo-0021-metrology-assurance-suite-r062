package assurance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/contracts"
	"example.com/solo-0021-metrology-assurance-suite/internal/domain"
	"example.com/solo-0021-metrology-assurance-suite/internal/repository"
)

const (
	statusCreated = 201
	statusOK      = 200
)

type Service struct {
	repository *repository.Repository
	now        func() time.Time
}

func NewService(repo *repository.Repository) *Service {
	return &Service{
		repository: repo,
		now:        time.Now,
	}
}

// RetryIdentity carries the stable retry identifier a caller attaches to one
// business submission. Key is the caller-supplied idempotency key. Scope binds
// the key to one write route (method plus routed path, including path values).
// Fingerprint is a stable hash of the submitted content, used to reject a key
// that is accidentally reused for a different request.
type RetryIdentity struct {
	Scope       string
	Key         string
	Fingerprint string
}

// Present reports whether the caller attached a stable retry identifier.
func (r RetryIdentity) Present() bool {
	return r.Scope != "" && r.Key != ""
}

// Submission is the result of running one write through the service. When a
// retry identifier resolves to the first successful submission, Replayed is
// true and Status/Payload reproduce that first, already readable result.
type Submission[T any] struct {
	Value    T
	Status   int
	Replayed bool
}

// submit runs one business action inside a single repository transaction. When
// the retry identity is absent it preserves the original, non-idempotent
// semantics. With a retry identity it:
//   - replays the stored result when the same key and content is seen again;
//   - returns ErrIdempotencyConflict when the same key is reused with content
//     that has a different fingerprint;
//   - stores the result together with the business changes only after the
//     action succeeds, so an action that fails before taking effect is never
//     remembered as a success.
//
// The repository mutation lock serializes concurrent retries, ensuring the
// stored record and the business fact are created by exactly one winner.
func submit[T any](
	s *Service,
	ctx context.Context,
	retry RetryIdentity,
	status int,
	act func(m *repository.Mutation) (T, error),
) (Submission[T], error) {
	var outcome Submission[T]
	err := s.repository.Mutate(ctx, func(m *repository.Mutation) error {
		if retry.Present() {
			if prior, exists := m.IdempotencyRecord(retry.Scope, retry.Key); exists {
				if prior.Fingerprint != retry.Fingerprint {
					return fmt.Errorf(
						"%w: key %s was already used with different request content",
						domain.ErrIdempotencyConflict, retry.Key,
					)
				}
				if err := json.Unmarshal(prior.Response, &outcome.Value); err != nil {
					return fmt.Errorf("replay stored response for key %s: %w", retry.Key, err)
				}
				outcome.Status = prior.StatusCode
				outcome.Replayed = true
				return nil
			}
		}

		value, err := act(m)
		if err != nil {
			return err
		}

		if retry.Present() {
			payload, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("encode idempotent response for key %s: %w", retry.Key, err)
			}
			record := repository.IdempotencyRecord{
				Scope:       retry.Scope,
				Key:         retry.Key,
				Fingerprint: retry.Fingerprint,
				StatusCode:  status,
				Response:    payload,
			}
			if err := m.PutIdempotencyRecord(record); err != nil {
				return err
			}
		}

		outcome.Value = value
		outcome.Status = status
		return nil
	})
	return outcome, err
}

// FingerprintRequest produces a stable hash of a decoded request body so that
// retries carrying the same logical content match regardless of JSON field
// order or whitespace, while genuinely different content is rejected.
func FingerprintRequest(input any) (string, error) {
	canonical, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("%w: cannot derive request fingerprint", domain.ErrInvalidInput)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) RegisterDevice(ctx context.Context, input contracts.RegisterDeviceRequest, retry RetryIdentity) (Submission[domain.Device], error) {
	return submit(s, ctx, retry, statusCreated, func(m *repository.Mutation) (domain.Device, error) {
		tag := domain.NormalizeAssetTag(input.AssetTag)
		if _, exists := m.DeviceByTag(tag); exists {
			return domain.Device{}, fmt.Errorf("%w: %s", domain.ErrDuplicateAssetTag, tag)
		}
		item, err := domain.NewDevice(
			m.NextID("device"),
			input.AssetTag,
			input.Model,
			input.Room,
			input.Steward,
			s.now(),
		)
		if err != nil {
			return domain.Device{}, err
		}
		if err := m.PutDevice(item); err != nil {
			return domain.Device{}, err
		}
		event, err := domain.NewAuditEvent(
			m.NextID("event"),
			"device",
			item.ID,
			"",
			"device_enrolled",
			item.Steward,
			"device enrolled for assurance work",
			s.now(),
		)
		if err != nil {
			return domain.Device{}, err
		}
		m.AppendEvent(event)
		return item, nil
	})
}

func (s *Service) Device(ctx context.Context, id string) (domain.Device, error) {
	var found domain.Device
	err := s.repository.Read(ctx, func(snapshot repository.Snapshot) error {
		item, exists := snapshot.Device(strings.TrimSpace(id))
		if !exists {
			return fmt.Errorf("%w: device %s", domain.ErrNotFound, id)
		}
		found = item
		return nil
	})
	return found, err
}

func (s *Service) OpenCase(ctx context.Context, input contracts.OpenCaseRequest, retry RetryIdentity) (Submission[contracts.CaseResponse], error) {
	return submit(s, ctx, retry, statusCreated, func(m *repository.Mutation) (contracts.CaseResponse, error) {
		deviceID := strings.TrimSpace(input.DeviceID)
		device, exists := m.Device(deviceID)
		if !exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: device %s", domain.ErrNotFound, deviceID)
		}
		if !device.IsActive() {
			return contracts.CaseResponse{}, fmt.Errorf("%w: device %s is %s", domain.ErrStateConflict, device.ID, device.Status)
		}
		if active, exists := m.ActiveCaseForDevice(device.ID); exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: case %s is still %s", domain.ErrActiveCaseExists, active.ID, active.State)
		}
		item, err := domain.NewVerificationCase(
			m.NextID("case"),
			device.ID,
			input.Nominal,
			input.Tolerance,
			input.Steward,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.PutCase(item); err != nil {
			return contracts.CaseResponse{}, err
		}
		event, err := domain.NewAuditEvent(
			m.NextID("event"),
			"case",
			item.ID,
			item.ID,
			"case_opened",
			item.Steward,
			"verification case opened",
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		m.AppendEvent(event)
		return caseResponse(m, item), nil
	})
}

func (s *Service) Case(ctx context.Context, id string) (contracts.CaseResponse, error) {
	var result contracts.CaseResponse
	err := s.repository.Read(ctx, func(snapshot repository.Snapshot) error {
		item, exists := snapshot.Case(strings.TrimSpace(id))
		if !exists {
			return fmt.Errorf("%w: case %s", domain.ErrNotFound, id)
		}
		result = caseResponseFromSnapshot(snapshot, item)
		return nil
	})
	return result, err
}

func (s *Service) RecordMeasurement(ctx context.Context, caseID string, input contracts.RecordMeasurementRequest, retry RetryIdentity) (Submission[contracts.CaseResponse], error) {
	return submit(s, ctx, retry, statusCreated, func(m *repository.Mutation) (contracts.CaseResponse, error) {
		id := strings.TrimSpace(caseID)
		item, exists := m.Case(id)
		if !exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: case %s", domain.ErrNotFound, id)
		}
		measurement, err := domain.NewMeasurement(
			m.NextID("measurement"),
			item.ID,
			input.Observed,
			item.Nominal,
			item.Tolerance,
			input.Technician,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := item.RecordMeasurement(measurement.Accepted, s.now()); err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.AppendMeasurement(measurement); err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.UpdateCase(item); err != nil {
			return contracts.CaseResponse{}, err
		}
		kind := "measurement_rejected"
		detail := "measurement exceeded tolerance and requires another measurement"
		if measurement.Accepted {
			kind = "measurement_accepted"
			detail = "measurement is within tolerance and ready for review"
		}
		event, err := domain.NewAuditEvent(
			m.NextID("event"),
			"case",
			item.ID,
			item.ID,
			kind,
			measurement.Technician,
			detail,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		m.AppendEvent(event)
		return caseResponse(m, item), nil
	})
}

func (s *Service) ReopenCase(ctx context.Context, caseID string, input contracts.ReopenCaseRequest, retry RetryIdentity) (Submission[contracts.CaseResponse], error) {
	return submit(s, ctx, retry, statusOK, func(m *repository.Mutation) (contracts.CaseResponse, error) {
		id := strings.TrimSpace(caseID)
		item, exists := m.Case(id)
		if !exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: case %s", domain.ErrNotFound, id)
		}
		if err := item.Reopen(s.now()); err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.UpdateCase(item); err != nil {
			return contracts.CaseResponse{}, err
		}
		actor := strings.TrimSpace(input.Actor)
		reason := strings.TrimSpace(input.Reason)
		if actor == "" || reason == "" {
			return contracts.CaseResponse{}, fmt.Errorf("%w: actor and reason are required", domain.ErrInvalidInput)
		}
		event, err := domain.NewAuditEvent(
			m.NextID("event"),
			"case",
			item.ID,
			item.ID,
			"case_reopened",
			actor,
			reason,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		m.AppendEvent(event)
		return caseResponse(m, item), nil
	})
}

func (s *Service) SealCase(ctx context.Context, caseID string, input contracts.SealCaseRequest, retry RetryIdentity) (Submission[contracts.CaseResponse], error) {
	return submit(s, ctx, retry, statusCreated, func(m *repository.Mutation) (contracts.CaseResponse, error) {
		id := strings.TrimSpace(caseID)
		item, exists := m.Case(id)
		if !exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: case %s", domain.ErrNotFound, id)
		}
		if _, exists := m.Seal(item.ID); exists {
			return contracts.CaseResponse{}, fmt.Errorf("%w: case %s", domain.ErrSealAlreadyCreated, item.ID)
		}
		measurement, ok := bestAcceptedMeasurement(m.Measurements(item.ID))
		if !ok {
			return contracts.CaseResponse{}, fmt.Errorf("%w: an accepted measurement is required before sealing", domain.ErrStateConflict)
		}
		if err := item.Seal(s.now()); err != nil {
			return contracts.CaseResponse{}, err
		}
		seal, err := domain.NewEvidenceSeal(
			m.NextID("seal"),
			item.ID,
			item.DeviceID,
			measurement.ID,
			input.Reviewer,
			input.Summary,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.PutSeal(seal); err != nil {
			return contracts.CaseResponse{}, err
		}
		if err := m.UpdateCase(item); err != nil {
			return contracts.CaseResponse{}, err
		}
		event, err := domain.NewAuditEvent(
			m.NextID("event"),
			"case",
			item.ID,
			item.ID,
			"case_sealed",
			seal.Reviewer,
			seal.Summary,
			s.now(),
		)
		if err != nil {
			return contracts.CaseResponse{}, err
		}
		m.AppendEvent(event)
		return caseResponse(m, item), nil
	})
}

func bestAcceptedMeasurement(items []domain.Measurement) (domain.Measurement, bool) {
	var selected domain.Measurement
	found := false
	for _, item := range items {
		if !item.Accepted {
			continue
		}
		if !found || item.AbsoluteDeviation() < selected.AbsoluteDeviation() {
			selected = item
			found = true
		}
	}
	return selected, found
}

func caseResponse(m *repository.Mutation, item domain.VerificationCase) contracts.CaseResponse {
	result := contracts.CaseResponse{
		Case:         item,
		Measurements: m.Measurements(item.ID),
		Events:       m.Events(item.ID),
	}
	if seal, exists := m.Seal(item.ID); exists {
		result.Seal = &seal
	}
	return result
}

func caseResponseFromSnapshot(snapshot repository.Snapshot, item domain.VerificationCase) contracts.CaseResponse {
	result := contracts.CaseResponse{
		Case:         item,
		Measurements: snapshot.Measurements(item.ID),
		Events:       snapshot.Events(item.ID),
	}
	if seal, exists := snapshot.Seal(item.ID); exists {
		result.Seal = &seal
	}
	return result
}
