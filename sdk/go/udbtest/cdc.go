package udbtest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type consumerCursor struct {
	owner string
	index int // Position in the retained canonical journal; never goes backward.
	id    string
}
type fakeDlq struct {
	identity        Identity
	event           Event
	record          *entityv1.DlqEventRecord
	failures        map[string]any
	failureMetadata map[string]any
}

func consumerOwner(identity Identity) (string, error) {
	if identity.ServiceIdentity != "" {
		return "service:" + identity.ServiceIdentity, nil
	}
	if identity.UserID != "" {
		return "user:" + identity.UserID, nil
	}
	return "", refuse(codes.Unauthenticated, "", "named CDC consumer requires a verified user or service identity")
}
func consumerName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) < 1 || len(name) > 120 {
		return "", refuse(codes.InvalidArgument, "", "invalid consumer_name length")
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("_.:-", c)) {
			return "", refuse(codes.InvalidArgument, "", "invalid consumer_name character")
		}
	}
	return name, nil
}
func cursorKey(identity Identity, consumer, pattern string) string {
	return identity.TenantID + "\x00" + identity.ProjectID + "\x00" + consumer + "\x00" + pattern
}
func cursorOwnerError() error {
	return refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "named CDC consumer belongs to another verified principal")
}
func eventInScope(event Event, identity Identity, pattern string) bool {
	project, _ := event.Envelope["project_id"].(string)
	return event.Envelope["tenant_id"] == identity.TenantID && project == identity.ProjectID && topicMatches(pattern, event.Topic)
}
func topicPattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "*"
	}
	return pattern
}

// PublishCDC retains a named consumer's verified owner, uses its persisted
// position, and revalidates authorization/expiry throughout the stream.
func (f *Fake) PublishCDC(req *entityv1.CDCSubscriptionRequest, stream grpc.ServerStreamingServer[eventsv1.CDCEnvelope]) error {
	ctx := stream.Context()
	if err := bindBody(ctx, req.GetContext()); err != nil {
		return err
	}
	identity := callerIdentity(ctx)
	pattern := topicPattern(req.GetTopicPattern())
	if err := f.authorize(ctx, pattern, "PublishCDC"); err != nil {
		return err
	}
	name := ""
	owner := ""
	var err error
	if strings.TrimSpace(req.GetConsumerName()) != "" {
		name, err = consumerName(req.GetConsumerName())
		if err != nil {
			return err
		}
		owner, err = consumerOwner(identity)
		if err != nil {
			return err
		}
	}
	f.mu.Lock()
	next, setupErr := func() (int, error) {
		next := 0
		if name != "" {
			key := cursorKey(identity, name, pattern)
			cursor, exists := f.cursors[key]
			if exists && cursor.owner != owner {
				return 0, cursorOwnerError()
			}
			if !exists {
				cursor = consumerCursor{owner: owner, index: -1}
				f.cursors[key] = cursor
			}
			if req.GetSinceEventId() == "" {
				next = cursor.index + 1
			}
		}
		if since := req.GetSinceEventId(); since != "" {
			for i, event := range f.events {
				if event.Envelope["event_id"] == since && eventInScope(event, identity, pattern) {
					return i + 1, nil
				}
			}
			return 0, refuse(codes.NotFound, "", "CDC resume event is unknown or outside the verified scope")
		}
		return next, nil
	}()
	f.mu.Unlock()
	if setupErr != nil {
		return setupErr
	}
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		verified, _ := ctx.Value(fakeIdentityKey{}).(fakeToken)
		if !time.Now().Before(verified.expires) {
			return refuse(codes.Unauthenticated, "", "bearer expired during CDC stream")
		}
		if err := f.authorize(ctx, pattern, "PublishCDC"); err != nil {
			return err
		}
		f.mu.Lock()
		pending := append([]Event(nil), f.events[next:]...)
		next = len(f.events)
		f.mu.Unlock()
		for _, event := range pending {
			if !eventInScope(event, identity, pattern) {
				continue
			}
			raw, err := json.Marshal(event.Envelope)
			if err != nil {
				return refuse(codes.Internal, "", "stored CDC event is not JSON")
			}
			id, _ := event.Envelope["event_id"].(string)
			if err := stream.Send(&eventsv1.CDCEnvelope{EventId: id, Topic: event.Topic, PartitionKey: event.PartitionKey, PayloadJson: string(raw)}); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
		}
	}
}

func (f *Fake) AckCdcEvents(ctx context.Context, req *entityv1.AckCdcEventsRequest) (*entityv1.AckCdcEventsResponse, error) {
	return f.settle(ctx, req, nil)
}

// NackCdcEvents quarantines the canonical event and advances only this
// consumer in one critical section. An injected refusal happens before either.
func (f *Fake) NackCdcEvents(ctx context.Context, req *entityv1.NackCdcEventsRequest) (*entityv1.AckCdcEventsResponse, error) {
	if req.GetAttempts() < 1 || req.GetAttempts() > 1000 || len(req.GetFailureReason()) > 2048 || !utf8.ValidString(req.GetFailureReason()) {
		return nil, refuse(codes.InvalidArgument, "", "invalid handler attempts or diagnostic length")
	}
	return f.settle(ctx, &entityv1.AckCdcEventsRequest{Context: req.GetContext(), ConsumerName: req.GetConsumerName(), TopicPattern: req.GetTopicPattern(), EventId: req.GetEventId()}, req)
}
func (f *Fake) settle(ctx context.Context, req *entityv1.AckCdcEventsRequest, failure *entityv1.NackCdcEventsRequest) (*entityv1.AckCdcEventsResponse, error) {
	name, err := consumerName(req.GetConsumerName())
	if err != nil {
		return nil, err
	}
	if !uuidText(req.GetEventId()) {
		return nil, refuse(codes.InvalidArgument, "", "event_id must be a UUID")
	}
	pattern := topicPattern(req.GetTopicPattern())
	if err := f.authorize(ctx, pattern, "PublishCDC"); err != nil {
		return nil, err
	}
	identity := callerIdentity(ctx)
	owner, err := consumerOwner(identity)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	index := -1
	for i, event := range f.events {
		if event.Envelope["event_id"] == req.GetEventId() && eventInScope(event, identity, pattern) {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "acknowledged CDC event is not retained in the verified scope")
	}
	key := cursorKey(identity, name, pattern)
	cursor, exists := f.cursors[key]
	if exists && cursor.owner != owner {
		return nil, cursorOwnerError()
	}
	if !exists {
		cursor = consumerCursor{owner: owner, index: -1}
	}
	if failure != nil {
		event := f.events[index]
		entry := f.dlq[req.GetEventId()]
		now := time.Now().Unix()
		if entry == nil {
			id, err := defaultValue("gen_random_uuid()")
			if err != nil {
				return nil, refuse(codes.Internal, "", "DLQ ID generation failed")
			}
			entry = &fakeDlq{identity: identity, event: event, failures: map[string]any{},
				failureMetadata: map[string]any{"service": "udb-cdc-consumer", "error_type": "CONSUMER_HANDLER_FAILED", "error_message": failure.GetFailureReason(), "failed_at": time.Now().UTC().Format(time.RFC3339Nano), "retry_count": 0},
				record:          &entityv1.DlqEventRecord{DlqId: fmt.Sprint(id), EventId: req.GetEventId(), Topic: event.Topic, ErrorType: "CONSUMER_HANDLER_FAILED", ErrorMessage: failure.GetFailureReason(), CreatedAtUnix: now}}
		}
		hash := sha256.New()
		for _, part := range []string{owner, name, pattern} {
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(part)))
			_, _ = hash.Write(size[:])
			_, _ = hash.Write([]byte(part))
		}
		failureKey := hex.EncodeToString(hash.Sum(nil))
		if _, settled := entry.failures[failureKey]; !settled {
			failures := copyRow(entry.failures)
			failures[failureKey] = map[string]any{"owner_identity": owner, "consumer_name": name, "topic_pattern": pattern, "attempts": failure.GetAttempts(), "failure_reason": failure.GetFailureReason(), "failed_at": time.Now().UTC().Format(time.RFC3339Nano)}
			raw, err := json.Marshal(map[string]any{"failed_event": event.Envelope, "failure_metadata": entry.failureMetadata, "consumer_failures": failures})
			if err != nil {
				return nil, refuse(codes.Internal, "", "failed event cannot be retained in DLQ")
			}
			entry.failures = failures
			entry.record.PayloadJson = raw
			entry.record.Status = "QUARANTINED"
			entry.record.UpdatedAtUnix = now
			f.dlq[req.GetEventId()] = entry
		}
	}
	if index > cursor.index {
		cursor.index, cursor.id = index, req.GetEventId()
	}
	f.cursors[key] = cursor
	return &entityv1.AckCdcEventsResponse{ConsumerName: name, TopicPattern: pattern, EventId: req.GetEventId(), AckedAtUnix: time.Now().Unix()}, nil
}

func uuidText(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}
func sameDomain(a, b Identity) bool { return a.TenantID == b.TenantID && a.ProjectID == b.ProjectID }
func (f *Fake) dlqEntry(ctx context.Context, id string) (*fakeDlq, error) {
	for _, entry := range f.dlq {
		if entry.record.GetDlqId() == id && sameDomain(entry.identity, callerIdentity(ctx)) {
			return entry, nil
		}
	}
	return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED", "DLQ event not found in verified tenant/project")
}
func (f *Fake) ListDlqEvents(ctx context.Context, req *entityv1.DlqListRequest) (*entityv1.DlqListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var events []*entityv1.DlqEventRecord
	for _, entry := range f.dlq {
		if sameDomain(entry.identity, callerIdentity(ctx)) && (req.GetTopic() == "" || req.GetTopic() == entry.record.GetTopic()) &&
			(req.GetStatusFilter() == "" || req.GetStatusFilter() == entry.record.GetStatus()) {
			events = append(events, proto.Clone(entry.record).(*entityv1.DlqEventRecord))
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].GetDlqId() < events[j].GetDlqId() })
	offset := 0
	if req.GetPageToken() != "" {
		var err error
		offset, err = strconv.Atoi(req.GetPageToken())
		if err != nil || offset < 0 || offset > len(events) {
			return nil, refuse(codes.InvalidArgument, "", "invalid DLQ page token")
		}
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	end := min(len(events), offset+limit)
	out := &entityv1.DlqListResponse{Events: events[offset:end], TotalCount: int32(len(events))}
	if end < len(events) {
		out.NextPageToken = strconv.Itoa(end)
	}
	return out, nil
}
func (f *Fake) GetDlqEvent(ctx context.Context, req *entityv1.DlqEventRequest) (*entityv1.DlqEventResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, err := f.dlqEntry(ctx, req.GetDlqId())
	if err != nil {
		return nil, err
	}
	return &entityv1.DlqEventResponse{Event: proto.Clone(entry.record).(*entityv1.DlqEventRecord)}, nil
}
func (f *Fake) ReplayDlqEvent(ctx context.Context, req *entityv1.DlqActionRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, err := f.dlqEntry(ctx, req.GetDlqId())
	if err != nil {
		return nil, err
	}
	if state := entry.record.GetStatus(); state != "OPEN" && state != "RETRYING" && state != "QUARANTINED" {
		return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED", "DLQ event is not replayable")
	}
	event := Event{Topic: entry.event.Topic, PartitionKey: entry.event.PartitionKey, Envelope: copyRow(entry.event.Envelope)}
	if !req.GetPreserveEventId() {
		id, err := defaultValue("gen_random_uuid()")
		if err != nil {
			return nil, refuse(codes.Internal, "", "replay ID generation failed")
		}
		event.Envelope["event_id"] = id
	} else {
		// The canonical outbox's event ID is unique; preserve never adds a
		// duplicate retained event in the fake journal.
		entry.record.Status = "REPLAYED"
		entry.record.UpdatedAtUnix = time.Now().Unix()
		return &entityv1.MutationResponse{MutationId: fmt.Sprint(event.Envelope["event_id"]), AffectedRows: 1}, nil
	}
	f.events = append(f.events, event)
	entry.record.Status = "REPLAYED"
	entry.record.UpdatedAtUnix = time.Now().Unix()
	return &entityv1.MutationResponse{MutationId: fmt.Sprint(event.Envelope["event_id"]), AffectedRows: 1}, nil
}
func (f *Fake) DismissDlqEvent(ctx context.Context, req *entityv1.DlqActionRequest) (*entityv1.MutationResponse, error) {
	return f.setDlqStatus(ctx, req, "DISMISSED")
}
func (f *Fake) QuarantineDlqEvent(ctx context.Context, req *entityv1.DlqActionRequest) (*entityv1.MutationResponse, error) {
	return f.setDlqStatus(ctx, req, "QUARANTINED")
}
func (f *Fake) setDlqStatus(ctx context.Context, req *entityv1.DlqActionRequest, state string) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, err := f.dlqEntry(ctx, req.GetDlqId())
	if err != nil {
		return nil, err
	}
	entry.record.Status = state
	entry.record.UpdatedAtUnix = time.Now().Unix()
	return &entityv1.MutationResponse{MutationId: req.GetDlqId(), AffectedRows: 1}, nil
}
