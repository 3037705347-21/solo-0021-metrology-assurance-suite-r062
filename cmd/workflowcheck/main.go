package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/bootstrap"
)

type apiResponse struct {
	Status int
	Body   map[string]any
	Raw    string
}

type harness struct {
	baseURL string
	client  *http.Client
	server  *httptest.Server
}

func main() {
	workflow := flag.String("workflow", "", "workflow id to verify")
	flag.Parse()

	checks := map[string]func() error{
		"register-device":          checkRegisterDevice,
		"open-verification-case":   checkOpenVerificationCase,
		"record-measurement":       checkRecordMeasurement,
		"reopen-verification-case": checkReopenVerificationCase,
		"seal-verification":        checkSealVerification,
		"idempotent-retry":         checkIdempotentRetry,
	}
	run, ok := checks[*workflow]
	if !ok {
		fmt.Fprintf(flag.CommandLine.Output(), "unknown workflow %q\n", *workflow)
		flag.Usage()
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Printf("workflow %s: failed: %v\n", *workflow, err)
		os.Exit(1)
	}
	fmt.Printf("workflow %s: ok\n", *workflow)
}

func checkRegisterDevice() error {
	h := newHarness()
	defer h.close()

	response, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": " met-1001 ",
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return err
	}
	id, err := nestedString(response.Body, "device", "id")
	if err != nil {
		return err
	}
	tag, err := nestedString(response.Body, "device", "asset_tag")
	if err != nil {
		return err
	}
	if tag != "MET-1001" {
		return fmt.Errorf("asset tag was not normalized: %q", tag)
	}

	read, err := h.request(http.MethodGet, "/devices/"+id, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}
	readID, err := nestedString(read.Body, "device", "id")
	if err != nil {
		return err
	}
	if readID != id {
		return fmt.Errorf("read returned device %q, expected %q", readID, id)
	}

	duplicate, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": "MET-1001",
		"model":     "Other Meter",
		"room":      "Room B",
		"steward":   "Mira",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(duplicate, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(duplicate, "duplicate_asset_tag")
}

func checkOpenVerificationCase() error {
	h := newHarness()
	defer h.close()

	deviceID, err := h.createDevice("MET-1101")
	if err != nil {
		return err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return err
	}
	state, err := nestedString(opened.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "awaiting_measurement" {
		return fmt.Errorf("opened case state = %q", state)
	}

	read, err := h.request(http.MethodGet, "/cases/"+caseID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}

	second, err := h.request(http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID,
		"nominal":   25.0,
		"tolerance": 0.5,
		"steward":   "Qiao",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(second, "active_case_exists")
}

func checkRecordMeasurement() error {
	h := newHarness()
	defer h.close()

	deviceID, err := h.createDevice("MET-1201")
	if err != nil {
		return err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return err
	}
	accepted, err := h.recordMeasurement(caseID, 25.2, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(accepted, http.StatusCreated); err != nil {
		return err
	}
	state, err := nestedString(accepted.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "review_ready" {
		return fmt.Errorf("accepted measurement state = %q", state)
	}
	acceptedValue, err := nestedBool(accepted.Body, "measurements", 0, "accepted")
	if err != nil {
		return err
	}
	if !acceptedValue {
		return errors.New("in-tolerance measurement was not accepted")
	}

	second, err := h.recordMeasurement(caseID, 25.1, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(second, "state_conflict"); err != nil {
		return err
	}

	rejectDeviceID, err := h.createDevice("MET-1202")
	if err != nil {
		return err
	}
	rejectCase, err := h.openCase(rejectDeviceID)
	if err != nil {
		return err
	}
	rejectCaseID, err := nestedString(rejectCase.Body, "case", "id")
	if err != nil {
		return err
	}
	rejected, err := h.recordMeasurement(rejectCaseID, 26.5, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(rejected, http.StatusCreated); err != nil {
		return err
	}
	rejectedState, err := nestedString(rejected.Body, "case", "state")
	if err != nil {
		return err
	}
	if rejectedState != "recheck_required" {
		return fmt.Errorf("out-of-tolerance state = %q", rejectedState)
	}
	rejectedValue, err := nestedBool(rejected.Body, "measurements", 0, "accepted")
	if err != nil {
		return err
	}
	if rejectedValue {
		return errors.New("out-of-tolerance measurement was accepted")
	}
	return nil
}

func checkReopenVerificationCase() error {
	h := newHarness()
	defer h.close()

	caseID, err := h.rejectedCase("MET-1301")
	if err != nil {
		return err
	}
	reopened, err := h.request(http.MethodPost, "/cases/"+caseID+"/reopen", map[string]any{
		"actor":  "Lin",
		"reason": "measurement setup was replaced",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(reopened, http.StatusOK); err != nil {
		return err
	}
	state, err := nestedString(reopened.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "awaiting_measurement" {
		return fmt.Errorf("reopened case state = %q", state)
	}
	measurements, err := nestedSlice(reopened.Body, "measurements")
	if err != nil {
		return err
	}
	if len(measurements) != 1 {
		return fmt.Errorf("reopen changed measurement history: %d entries", len(measurements))
	}

	second, err := h.request(http.MethodPost, "/cases/"+caseID+"/reopen", map[string]any{
		"actor":  "Lin",
		"reason": "duplicate request",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(second, "state_conflict"); err != nil {
		return err
	}

	events, err := h.request(http.MethodGet, "/cases/"+caseID+"/events", nil)
	if err != nil {
		return err
	}
	if err := expectStatus(events, http.StatusOK); err != nil {
		return err
	}
	if !hasEvent(events.Body, "case_reopened") {
		return errors.New("reopen audit event is missing")
	}
	return nil
}

func checkSealVerification() error {
	h := newHarness()
	defer h.close()

	caseID, err := h.readyCase("MET-1401")
	if err != nil {
		return err
	}
	type outcome struct {
		response apiResponse
		err      error
	}
	results := make(chan outcome, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			response, err := h.request(http.MethodPost, "/cases/"+caseID+"/seal", map[string]any{
				"reviewer": fmt.Sprintf("Reviewer-%d", index+1),
				"summary":  "accepted measurement sealed for review",
			})
			results <- outcome{response: response, err: err}
		}(index)
	}
	wait.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for item := range results {
		if item.err != nil {
			return item.err
		}
		switch item.response.Status {
		case http.StatusCreated:
			successes++
		case http.StatusConflict:
			conflicts++
		default:
			return fmt.Errorf("unexpected concurrent seal status %d: %s", item.response.Status, item.response.Raw)
		}
	}
	if successes != 1 || conflicts != 1 {
		return fmt.Errorf("concurrent seal outcomes: %d success, %d conflict", successes, conflicts)
	}

	read, err := h.request(http.MethodGet, "/cases/"+caseID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}
	state, err := nestedString(read.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "sealed" {
		return fmt.Errorf("sealed case state = %q", state)
	}
	sealID, err := nestedString(read.Body, "seal", "id")
	if err != nil {
		return err
	}
	if sealID == "" {
		return errors.New("evidence seal is missing")
	}
	return nil
}

func checkIdempotentRetry() error {
	h := newHarness()
	defer h.close()

	// Register device: a retry with the same key and content replays the first
	// result instead of registering again.
	registerBody := map[string]any{
		"asset_tag": "MET-2001",
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	}
	first, err := h.requestWithKey(http.MethodPost, "/devices", registerBody, "retry-register-1")
	if err != nil {
		return err
	}
	if err := expectStatus(first, http.StatusCreated); err != nil {
		return err
	}
	deviceID, err := nestedString(first.Body, "device", "id")
	if err != nil {
		return err
	}
	replay, err := h.requestWithKey(http.MethodPost, "/devices", registerBody, "retry-register-1")
	if err != nil {
		return err
	}
	if err := expectStatus(replay, http.StatusCreated); err != nil {
		return err
	}
	if replay.Raw != first.Raw {
		return fmt.Errorf("replayed register response differs:\nfirst:  %s\nreplay: %s", first.Raw, replay.Raw)
	}
	duplicate, err := h.requestWithKey(http.MethodPost, "/devices", registerBody, "retry-register-2")
	if err != nil {
		return err
	}
	if err := expectStatus(duplicate, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(duplicate, "duplicate_asset_tag"); err != nil {
		return fmt.Errorf("retry created a second device: %w", err)
	}

	// The same key with different content is rejected, and stays rejected.
	changedBody := map[string]any{
		"asset_tag": "MET-2001",
		"model":     "Other Meter",
		"room":      "Room B",
		"steward":   "Mira",
	}
	for attempt := 0; attempt < 2; attempt++ {
		conflict, err := h.requestWithKey(http.MethodPost, "/devices", changedBody, "retry-register-1")
		if err != nil {
			return err
		}
		if err := expectStatus(conflict, http.StatusConflict); err != nil {
			return err
		}
		if err := expectErrorCode(conflict, "idempotency_conflict"); err != nil {
			return err
		}
	}

	// A request that failed before taking effect is not remembered: the same
	// key may be reused with corrected content.
	invalidBody := map[string]any{
		"asset_tag": "MET-2002",
		"model":     "",
		"room":      "Room A",
		"steward":   "Lin",
	}
	failed, err := h.requestWithKey(http.MethodPost, "/devices", invalidBody, "retry-register-3")
	if err != nil {
		return err
	}
	if err := expectStatus(failed, http.StatusBadRequest); err != nil {
		return err
	}
	recoveredBody := map[string]any{
		"asset_tag": "MET-2002",
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	}
	recovered, err := h.requestWithKey(http.MethodPost, "/devices", recoveredBody, "retry-register-3")
	if err != nil {
		return err
	}
	if err := expectStatus(recovered, http.StatusCreated); err != nil {
		return err
	}
	recoveredReplay, err := h.requestWithKey(http.MethodPost, "/devices", recoveredBody, "retry-register-3")
	if err != nil {
		return err
	}
	if err := expectStatus(recoveredReplay, http.StatusCreated); err != nil {
		return err
	}
	if recoveredReplay.Raw != recovered.Raw {
		return fmt.Errorf("recovered register replay differs:\nfirst:  %s\nreplay: %s", recovered.Raw, recoveredReplay.Raw)
	}

	// Concurrent open-case retries with one key commit exactly one case.
	caseBody := map[string]any{
		"device_id": deviceID,
		"nominal":   25.0,
		"tolerance": 0.5,
		"steward":   "Qiao",
	}
	opened, err := h.concurrent(3, func() (apiResponse, error) {
		return h.requestWithKey(http.MethodPost, "/cases", caseBody, "retry-case-1")
	})
	if err != nil {
		return err
	}
	caseID := ""
	for index, response := range opened {
		if err := expectStatus(response, http.StatusCreated); err != nil {
			return fmt.Errorf("concurrent open %d: %w", index, err)
		}
		id, err := nestedString(response.Body, "case", "id")
		if err != nil {
			return err
		}
		if caseID == "" {
			caseID = id
		} else if id != caseID {
			return fmt.Errorf("concurrent opens committed cases %q and %q", caseID, id)
		}
	}
	secondCase, err := h.requestWithKey(http.MethodPost, "/cases", caseBody, "retry-case-2")
	if err != nil {
		return err
	}
	if err := expectStatus(secondCase, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(secondCase, "active_case_exists"); err != nil {
		return fmt.Errorf("concurrent retries committed more than one case: %w", err)
	}

	// Measurement replay returns the recorded outcome instead of a state
	// conflict, and appends no further measurement.
	measurementBody := map[string]any{
		"observed":   25.2,
		"technician": "Rui",
	}
	measured, err := h.requestWithKey(http.MethodPost, "/cases/"+caseID+"/measurements", measurementBody, "retry-measure-1")
	if err != nil {
		return err
	}
	if err := expectStatus(measured, http.StatusCreated); err != nil {
		return err
	}
	measuredReplay, err := h.requestWithKey(http.MethodPost, "/cases/"+caseID+"/measurements", measurementBody, "retry-measure-1")
	if err != nil {
		return err
	}
	if err := expectStatus(measuredReplay, http.StatusCreated); err != nil {
		return err
	}
	if measuredReplay.Raw != measured.Raw {
		return fmt.Errorf("replayed measurement response differs:\nfirst:  %s\nreplay: %s", measured.Raw, measuredReplay.Raw)
	}
	plainRetry, err := h.request(http.MethodPost, "/cases/"+caseID+"/measurements", measurementBody)
	if err != nil {
		return err
	}
	if err := expectStatus(plainRetry, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(plainRetry, "state_conflict"); err != nil {
		return fmt.Errorf("request without key lost the original semantics: %w", err)
	}
	caseView, err := h.request(http.MethodGet, "/cases/"+caseID, nil)
	if err != nil {
		return err
	}
	measurements, err := nestedSlice(caseView.Body, "measurements")
	if err != nil {
		return err
	}
	if len(measurements) != 1 {
		return fmt.Errorf("measurement replay appended entries: %d measurements", len(measurements))
	}

	// Reopen replay returns the recorded outcome and adds no extra audit event.
	rejectedCaseID, err := h.rejectedCase("MET-2003")
	if err != nil {
		return err
	}
	reopenBody := map[string]any{
		"actor":  "Lin",
		"reason": "measurement setup was replaced",
	}
	reopened, err := h.requestWithKey(http.MethodPost, "/cases/"+rejectedCaseID+"/reopen", reopenBody, "retry-reopen-1")
	if err != nil {
		return err
	}
	if err := expectStatus(reopened, http.StatusOK); err != nil {
		return err
	}
	reopenedReplay, err := h.requestWithKey(http.MethodPost, "/cases/"+rejectedCaseID+"/reopen", reopenBody, "retry-reopen-1")
	if err != nil {
		return err
	}
	if err := expectStatus(reopenedReplay, http.StatusOK); err != nil {
		return err
	}
	if reopenedReplay.Raw != reopened.Raw {
		return fmt.Errorf("replayed reopen response differs:\nfirst:  %s\nreplay: %s", reopened.Raw, reopenedReplay.Raw)
	}
	reopenEvents, err := h.request(http.MethodGet, "/cases/"+rejectedCaseID+"/events", nil)
	if err != nil {
		return err
	}
	if count := countEvents(reopenEvents.Body, "case_reopened"); count != 1 {
		return fmt.Errorf("reopen replay appended audit events: %d case_reopened events", count)
	}

	// Concurrent seal retries with one key commit exactly one evidence seal.
	sealBody := map[string]any{
		"reviewer": "Wei",
		"summary":  "accepted measurement sealed for review",
	}
	sealed, err := h.concurrent(2, func() (apiResponse, error) {
		return h.requestWithKey(http.MethodPost, "/cases/"+caseID+"/seal", sealBody, "retry-seal-1")
	})
	if err != nil {
		return err
	}
	sealID := ""
	for index, response := range sealed {
		if err := expectStatus(response, http.StatusCreated); err != nil {
			return fmt.Errorf("concurrent seal %d: %w", index, err)
		}
		id, err := nestedString(response.Body, "seal", "id")
		if err != nil {
			return err
		}
		if sealID == "" {
			sealID = id
		} else if id != sealID {
			return fmt.Errorf("concurrent seals committed seals %q and %q", sealID, id)
		}
	}
	sealedAgain, err := h.requestWithKey(http.MethodPost, "/cases/"+caseID+"/seal", sealBody, "retry-seal-2")
	if err != nil {
		return err
	}
	if err := expectStatus(sealedAgain, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(sealedAgain, "seal_exists"); err != nil {
		return fmt.Errorf("concurrent retries committed more than one seal: %w", err)
	}

	// A key is bound to one business intent across all write endpoints.
	crossOperation, err := h.requestWithKey(http.MethodPost, "/cases", caseBody, "retry-register-1")
	if err != nil {
		return err
	}
	if err := expectStatus(crossOperation, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(crossOperation, "idempotency_conflict")
}

func (h *harness) concurrent(count int, call func() (apiResponse, error)) ([]apiResponse, error) {
	type outcome struct {
		response apiResponse
		err      error
	}
	results := make(chan outcome, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := call()
			results <- outcome{response: response, err: err}
		}()
	}
	wait.Wait()
	close(results)
	responses := make([]apiResponse, 0, count)
	for item := range results {
		if item.err != nil {
			return nil, item.err
		}
		responses = append(responses, item.response)
	}
	return responses, nil
}

func countEvents(root map[string]any, kind string) int {
	items, err := nestedSlice(root, "events")
	if err != nil {
		return 0
	}
	count := 0
	for _, item := range items {
		event, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if event["kind"] == kind {
			count++
		}
	}
	return count
}

func newHarness() *harness {
	server := httptest.NewServer(bootstrap.NewHandler())
	return &harness{
		baseURL: server.URL,
		client:  server.Client(),
		server:  server,
	}
}

func (h *harness) close() {
	h.server.Close()
}

func (h *harness) request(method, path string, body any) (apiResponse, error) {
	return h.requestWithKey(method, path, body, "")
}

func (h *harness) requestWithKey(method, path string, body any, idempotencyKey string) (apiResponse, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return apiResponse{}, err
		}
		reader = bytes.NewReader(encoded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return apiResponse{}, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return apiResponse{}, err
	}
	defer response.Body.Close()
	rawBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return apiResponse{}, err
	}
	result := apiResponse{Status: response.StatusCode, Raw: string(rawBytes)}
	if len(bytes.TrimSpace(rawBytes)) > 0 {
		if err := json.Unmarshal(rawBytes, &result.Body); err != nil {
			return apiResponse{}, fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}
	return result, nil
}

func (h *harness) createDevice(tag string) (string, error) {
	response, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": tag,
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return nestedString(response.Body, "device", "id")
}

func (h *harness) openCase(deviceID string) (apiResponse, error) {
	response, err := h.request(http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID,
		"nominal":   25.0,
		"tolerance": 0.5,
		"steward":   "Qiao",
	})
	if err != nil {
		return apiResponse{}, err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return apiResponse{}, err
	}
	return response, nil
}

func (h *harness) recordMeasurement(caseID string, observed float64, technician string) (apiResponse, error) {
	return h.request(http.MethodPost, "/cases/"+caseID+"/measurements", map[string]any{
		"observed":   observed,
		"technician": technician,
	})
}

func (h *harness) rejectedCase(tag string) (string, error) {
	deviceID, err := h.createDevice(tag)
	if err != nil {
		return "", err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return "", err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return "", err
	}
	response, err := h.recordMeasurement(caseID, 26.5, "Rui")
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return caseID, nil
}

func (h *harness) readyCase(tag string) (string, error) {
	deviceID, err := h.createDevice(tag)
	if err != nil {
		return "", err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return "", err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return "", err
	}
	response, err := h.recordMeasurement(caseID, 25.2, "Rui")
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return caseID, nil
}

func expectStatus(response apiResponse, expected int) error {
	if response.Status != expected {
		return fmt.Errorf("status = %d, expected %d: %s", response.Status, expected, response.Raw)
	}
	return nil
}

func expectErrorCode(response apiResponse, expected string) error {
	code, err := nestedString(response.Body, "error", "code")
	if err != nil {
		return err
	}
	if code != expected {
		return fmt.Errorf("error code = %q, expected %q", code, expected)
	}
	return nil
}

func nestedString(root map[string]any, path ...string) (string, error) {
	value, err := nestedValue(root, path...)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s is not a string", strings.Join(path, "."))
	}
	return text, nil
}

func nestedBool(root map[string]any, path ...any) (bool, error) {
	value, err := nestedAny(root, path...)
	if err != nil {
		return false, err
	}
	flag, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%v is not a boolean", path)
	}
	return flag, nil
}

func nestedSlice(root map[string]any, key string) ([]any, error) {
	value, ok := root[key]
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s is not an array", key)
	}
	return items, nil
}

func hasEvent(root map[string]any, kind string) bool {
	items, err := nestedSlice(root, "events")
	if err != nil {
		return false
	}
	for _, item := range items {
		event, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if event["kind"] == kind {
			return true
		}
	}
	return false
}

func nestedValue(root map[string]any, path ...string) (any, error) {
	parts := make([]any, 0, len(path))
	for _, part := range path {
		parts = append(parts, part)
	}
	return nestedAny(root, parts...)
}

func nestedAny(root map[string]any, path ...any) (any, error) {
	var current any = root
	for _, part := range path {
		switch typed := part.(type) {
		case string:
			object, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s is not an object", typed)
			}
			value, ok := object[typed]
			if !ok {
				return nil, fmt.Errorf("missing %s", typed)
			}
			current = value
		case int:
			items, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("index %d is not inside an array", typed)
			}
			if typed < 0 || typed >= len(items) {
				return nil, fmt.Errorf("index %d is out of range", typed)
			}
			current = items[typed]
		default:
			return nil, fmt.Errorf("unsupported path element %T", part)
		}
	}
	return current, nil
}
