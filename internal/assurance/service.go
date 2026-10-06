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

// maxIdempotencyKeyLength bounds caller-supplied retry identifiers.
const maxIdempotencyKeyLength = 128

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

// mutateIdempotent runs fn inside one repository commit and makes the commit
// safe to retry when the caller supplies a stable idempotency key. The first
// successful execution stores its JSON-encoded result under the key; a retry
// with the same key and the same business intent replays that stored result
// without re-executing fn, while the same key with different content is
// rejected. A failed execution stores nothing, so later retries are evaluated
// fresh. An empty key disables the mechanism and preserves the original
// semantics. The lookup, execution, and recording share one mutation lock, so
// concurrent retries commit exactly one business fact.
func mutateIdempotent[T any](ctx context.Context, repo *repository.Repository, key, fingerprint string, fn func(*repository.Mutation) (T, error)) (T, error) {
	var result T
	if len(key) > maxIdempotencyKeyLength {
		return result, fmt.Errorf("%w: idempotency key exceeds %d characters", domain.ErrInvalidInput, maxIdempotencyKeyLength)
	}
	err := repo.Mutate(ctx, func(m *repository.Mutation) error {
		if key != "" {
			if record, exists := m.IdempotencyRecord(key); exists {
				if record.Fingerprint != fingerprint {
					return fmt.Errorf("%w: idempotency key %q", domain.ErrIdempotencyConflict, key)
				}
				return json.Unmarshal(record.Payload, &result)
			}
		}
		outcome, err := fn(m)
		if err != nil {
			return err
		}
		if key != "" {
			payload, err := json.Marshal(outcome)
			if err != nil {
				return err
			}
			record := repository.IdempotencyRecord{
				Key:         key,
				Fingerprint: fingerprint,
				Payload:     payload,
			}
			if err := m.PutIdempotencyRecord(record); err != nil {
				return err
			}
		}
		result = outcome
		return nil
	})
	return result, err
}

// submissionFingerprint binds one business intent to an idempotency key. The
// operation name and every input that defines the submission are hashed from
// their canonical JSON encoding, so a byte-level retry of the same intent
// matches while any changed content produces a different fingerprint.
func submissionFingerprint(operation string, inputs ...any) string {
	digest := sha256.New()
	digest.Write([]byte(operation))
	for _, input := range inputs {
		encoded, err := json.Marshal(input)
		if err != nil {
			encoded = []byte(fmt.Sprintf("%#v", input))
		}
		digest.Write([]byte{0})
		digest.Write(encoded)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func (s *Service) RegisterDevice(ctx context.Context, input contracts.RegisterDeviceRequest, key string) (domain.Device, error) {
	fingerprint := submissionFingerprint("register-device", input)
	return mutateIdempotent(ctx, s.repository, key, fingerprint, func(m *repository.Mutation) (domain.Device, error) {
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

func (s *Service) OpenCase(ctx context.Context, input contracts.OpenCaseRequest, key string) (contracts.CaseResponse, error) {
	fingerprint := submissionFingerprint("open-case", input)
	return mutateIdempotent(ctx, s.repository, key, fingerprint, func(m *repository.Mutation) (contracts.CaseResponse, error) {
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

func (s *Service) RecordMeasurement(ctx context.Context, caseID string, input contracts.RecordMeasurementRequest, key string) (contracts.CaseResponse, error) {
	id := strings.TrimSpace(caseID)
	fingerprint := submissionFingerprint("record-measurement", id, input)
	return mutateIdempotent(ctx, s.repository, key, fingerprint, func(m *repository.Mutation) (contracts.CaseResponse, error) {
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

func (s *Service) ReopenCase(ctx context.Context, caseID string, input contracts.ReopenCaseRequest, key string) (contracts.CaseResponse, error) {
	id := strings.TrimSpace(caseID)
	fingerprint := submissionFingerprint("reopen-case", id, input)
	return mutateIdempotent(ctx, s.repository, key, fingerprint, func(m *repository.Mutation) (contracts.CaseResponse, error) {
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

func (s *Service) SealCase(ctx context.Context, caseID string, input contracts.SealCaseRequest, key string) (contracts.CaseResponse, error) {
	id := strings.TrimSpace(caseID)
	fingerprint := submissionFingerprint("seal-case", id, input)
	return mutateIdempotent(ctx, s.repository, key, fingerprint, func(m *repository.Mutation) (contracts.CaseResponse, error) {
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
