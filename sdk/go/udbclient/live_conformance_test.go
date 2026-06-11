package udbclient

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const liveMessageType = "udb.sdk.live.v1.SdkLiveRecord"

func TestLiveGeneratedRPCSurface(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}

	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	meta := Metadata{
		TenantID:             tenant,
		ProjectID:            project,
		Purpose:              "go.live.conformance",
		CorrelationID:        "go-live-conformance",
		// No client-asserted scopes: admin authority comes from the Login JWT
		// (the broker derives scopes from the validated bearer, ignoring x-scopes
		// when a JWT verifier is configured). This is the real production path.
		ServiceIdentity:      "go.sdk.live",
		ClientCatalogVersion: ProtocolVersion,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer brokerConn.Close()
	authConn := brokerConn
	if authTarget != target {
		authConn, err = grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial auth: %v", err)
		}
		defer authConn.Close()
	}

	login, err := authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:    requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
		Password:    requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantHint:  tenant,
		ProjectHint: project,
		DeviceName:  "go-sdk-live-conformance",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	if login.GetAccessToken() == "" || login.GetRefreshToken() == "" {
		t.Fatalf("Login must return access and refresh tokens")
	}

	auth := NewAuthClient(authConn, meta)
	if _, err := auth.AuthenticateBearer(ctx, login.GetAccessToken()); err != nil {
		t.Fatalf("AuthenticateBearer rejected Login access token: %v", err)
	}
	if refreshed, err := auth.Authn.RefreshToken(auth.Context(ctx), &authnv1.RefreshTokenRequest{RefreshToken: login.GetRefreshToken()}); err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	} else if refreshed.GetAccessToken() == "" {
		t.Fatalf("RefreshToken must return a new access token")
	}

	authz := "Bearer " + login.GetAccessToken()
	brokerGen := NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
	authGen := NewGenerated(authConn, liveGeneratedOptions(meta, authz))

	caps, err := servicesv1.NewDataBrokerClient(brokerConn).GetCapabilities(
		brokerGen.outgoingContext(ctx),
		&entityv1.CapabilitiesRequest{},
	)
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}
	assertRequiredLiveBackends(t, caps.GetEnabledBackends())

	runLiveBackendE2E(t, ctx, servicesv1.NewDataBrokerClient(brokerConn), brokerGen.outgoingContext(ctx), tenant, project)

	probed := 0
	for _, rpc := range AllRPCs {
		gen := authGen
		if rpc.Service == "DataBroker" {
			gen = brokerGen
		}
		t.Run(rpc.FullMethod, func(t *testing.T) {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			err := probeLiveRPC(callCtx, gen, rpc)
			if isLiveMountFailure(err) {
				t.Fatalf("%s did not reach an implemented live RPC: %v", rpc.FullMethod, err)
			}
		})
		probed++
	}
	if probed != len(AllRPCs) {
		t.Fatalf("probed %d RPCs, want %d", probed, len(AllRPCs))
	}
}

func runLiveBackendE2E(t *testing.T, ctx context.Context, broker servicesv1.DataBrokerClient, callCtx context.Context, tenant, project string) {
	t.Helper()
	suffix := strings.NewReplacer(".", "-", ":", "-", "+", "-").Replace(time.Now().UTC().Format("20060102T150405.000000000"))
	recordID := "go-" + suffix
	secondRecordID := "go-batch-" + suffix
	lookupKey := "go-live-" + suffix
	collection := "sdk_live_docs_" + strings.NewReplacer("-", "_").Replace(suffix)
	documentID := "doc-" + suffix
	bucket := liveEnv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
	objectKey := "go/" + suffix + ".txt"
	objectBody := []byte("go live sdk object " + suffix)
	requestCtx := liveRequestContext(tenant, project, "go.live.backend.e2e")

	if _, err := broker.GenericDispatch(callCtx, &entityv1.GenericDispatchRequest{
		Context:  requestCtx,
		Backend:  "postgres",
		Operation: "query",
		SpecJson:  `{"sql":"SELECT 1::INT AS live_probe"}`,
	}); err != nil {
		t.Fatalf("postgres GenericDispatch query failed: %v", err)
	}

	inserted, err := broker.Upsert(callCtx, &entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, lookupKey, "created-from-go", 1),
		ConflictFields: []string{"record_id"},
		ReturnRecord:   true,
	})
	if err != nil {
		t.Fatalf("typed Postgres Upsert insert failed: %v", err)
	}
	if inserted.GetAffectedRows() != 1 || liveMutationPayload(t, inserted) != "created-from-go" {
		t.Fatalf("insert response = affected %d payload %q", inserted.GetAffectedRows(), liveMutationPayload(t, inserted))
	}

	selected, err := broker.Select(callCtx, &entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("typed Postgres Select failed: %v", err)
	}
	if liveRecordPayload(t, selected, 0) != "created-from-go" {
		t.Fatalf("select payload = %q", liveRecordPayload(t, selected, 0))
	}

	updated, err := broker.Upsert(callCtx, &entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, lookupKey, "updated-from-go", 2),
		ConflictFields: []string{"record_id"},
		ReturnRecord:   true,
	})
	if err != nil {
		t.Fatalf("typed Postgres Upsert update failed: %v", err)
	}
	if liveMutationPayload(t, updated) != "updated-from-go" {
		t.Fatalf("update payload = %q", liveMutationPayload(t, updated))
	}

	selectV2, err := broker.SelectV2(callCtx, &entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("typed Postgres SelectV2 open failed: %v", err)
	}
	if _, err := selectV2.Recv(); err != nil {
		t.Fatalf("typed Postgres SelectV2 receive failed: %v", err)
	}

	upsertStream, err := broker.BatchUpsert(callCtx)
	if err != nil {
		t.Fatalf("BatchUpsert open failed: %v", err)
	}
	if err := upsertStream.Send(&entityv1.UpsertRequest{
		Context:        requestCtx,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, secondRecordID, tenant, project, lookupKey+"-batch", "created-from-go-batch", 1),
		ConflictFields: []string{"record_id"},
	}); err != nil {
		t.Fatalf("BatchUpsert send failed: %v", err)
	}
	if _, err := upsertStream.Recv(); err != nil {
		t.Fatalf("BatchUpsert recv failed: %v", err)
	}
	_ = upsertStream.CloseSend()

	selectStream, err := broker.BatchSelect(callCtx)
	if err != nil {
		t.Fatalf("BatchSelect open failed: %v", err)
	}
	if err := selectStream.Send(&entityv1.SelectRequest{
		Context:     requestCtx,
		MessageType: liveMessageType,
		Filter:      liveStruct(t, map[string]any{"record_id": secondRecordID, "tenant_id": tenant, "project_id": project}),
		Limit:       1,
	}); err != nil {
		t.Fatalf("BatchSelect send failed: %v", err)
	}
	batchSelected, err := selectStream.Recv()
	if err != nil {
		t.Fatalf("BatchSelect recv failed: %v", err)
	}
	_ = selectStream.CloseSend()
	if liveRecordPayload(t, batchSelected, 0) != "created-from-go-batch" {
		t.Fatalf("BatchSelect payload = %q", liveRecordPayload(t, batchSelected, 0))
	}

	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context:      requestCtx,
		Backend:      "mongodb",
		ResourceName: collection,
		SpecJson:     `{"collection":"` + collection + `"}`,
	}); err != nil {
		t.Fatalf("Mongo EnsureResource failed: %v", err)
	}
	resources, err := broker.ListResources(callCtx, &entityv1.ResourceAdminRequest{Context: requestCtx, Backend: "mongodb"})
	if err != nil {
		t.Fatalf("Mongo ListResources failed: %v", err)
	}
	if !containsResource(resources.GetResources(), collection) {
		t.Fatalf("Mongo resources %v missing %s", resources.GetResources(), collection)
	}

	_, err = broker.DocumentUpsert(callCtx, &entityv1.DocumentUpsertRequest{
		Context:    requestCtx,
		Resource:   &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection},
		DocumentId: documentID,
		Document: liveStruct(t, map[string]any{
			"_id": documentID, "tenant_id": tenant, "project_id": project, "payload": "mongo-created", "revision": 1,
		}),
	})
	if err != nil {
		t.Fatalf("Mongo DocumentUpsert insert failed: %v", err)
	}
	gotDoc, err := broker.DocumentGet(callCtx, &entityv1.DocumentGetRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
	})
	if err != nil {
		t.Fatalf("Mongo DocumentGet failed: %v", err)
	}
	if liveDocPayload(gotDoc) != "mongo-created" {
		t.Fatalf("Mongo DocumentGet payload = %q", liveDocPayload(gotDoc))
	}
	if _, err := broker.DocumentUpsert(callCtx, &entityv1.DocumentUpsertRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
		Document: liveStruct(t, map[string]any{"payload": "mongo-updated", "revision": 2}),
	}); err != nil {
		t.Fatalf("Mongo DocumentUpsert update failed: %v", err)
	}
	foundDoc, err := broker.DocumentFind(callCtx, &entityv1.DocumentFindRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, Filter: liveStruct(t, map[string]any{"_id": documentID}), Limit: 1,
	})
	if err != nil {
		t.Fatalf("Mongo DocumentFind failed: %v", err)
	}
	if liveDocPayload(foundDoc) != "mongo-updated" {
		t.Fatalf("Mongo DocumentFind payload = %q", liveDocPayload(foundDoc))
	}
	if deletedDoc, err := broker.DocumentDelete(callCtx, &entityv1.DocumentDeleteRequest{
		Context: requestCtx, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: documentID,
	}); err != nil {
		t.Fatalf("Mongo DocumentDelete failed: %v", err)
	} else if deletedDoc.GetAffectedRows() != 1 {
		t.Fatalf("Mongo DocumentDelete affected_rows=%d", deletedDoc.GetAffectedRows())
	}

	if _, err := broker.EnsureResource(callCtx, &entityv1.ResourceAdminRequest{
		Context: requestCtx, Backend: "minio", ResourceName: bucket, SpecJson: `{}`,
	}); err != nil {
		t.Fatalf("MinIO EnsureResource failed: %v", err)
	}
	put, err := broker.PutObject(callCtx)
	if err != nil {
		t.Fatalf("PutObject open failed: %v", err)
	}
	if err := put.Send(&entityv1.Chunk{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Data: objectBody[:10], ContentType: "text/plain"}); err != nil {
		t.Fatalf("PutObject first send failed: %v", err)
	}
	if err := put.Send(&entityv1.Chunk{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Data: objectBody[10:], FinalChunk: true}); err != nil {
		t.Fatalf("PutObject final send failed: %v", err)
	}
	putResp, err := put.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutObject close failed: %v", err)
	}
	if putResp.GetAffectedRows() != 1 {
		t.Fatalf("PutObject affected_rows=%d", putResp.GetAffectedRows())
	}
	get, err := broker.GetObject(callCtx, &entityv1.ObjectRequest{Context: requestCtx, Bucket: bucket, ObjectKey: objectKey})
	if err != nil {
		t.Fatalf("GetObject open failed: %v", err)
	}
	downloaded := []byte{}
	for {
		chunk, err := get.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("GetObject recv failed: %v", err)
		}
		downloaded = append(downloaded, chunk.GetData()...)
	}
	if string(downloaded) != string(objectBody) {
		t.Fatalf("GetObject body = %q", string(downloaded))
	}
	if presigned, err := broker.GeneratePresignedUrl(callCtx, &entityv1.UrlRequest{
		Context: requestCtx, Bucket: bucket, ObjectKey: objectKey, Method: "GET", TtlSeconds: 60,
	}); err != nil {
		t.Fatalf("GeneratePresignedUrl failed: %v", err)
	} else if !strings.HasPrefix(presigned.GetUrl(), "http") {
		t.Fatalf("GeneratePresignedUrl returned %q", presigned.GetUrl())
	}

	if deleted, err := broker.Delete(callCtx, &entityv1.DeleteRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}),
	}); err != nil {
		t.Fatalf("typed Postgres Delete failed: %v", err)
	} else if deleted.GetAffectedRows() != 1 {
		t.Fatalf("typed Postgres Delete affected_rows=%d", deleted.GetAffectedRows())
	}
	_, _ = broker.Delete(callCtx, &entityv1.DeleteRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": secondRecordID, "tenant_id": tenant, "project_id": project}),
	})
	afterDelete, err := broker.Select(callCtx, &entityv1.SelectRequest{
		Context: requestCtx, MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID, "tenant_id": tenant, "project_id": project}), Limit: 1,
	})
	if err != nil {
		t.Fatalf("typed Postgres Select after Delete failed: %v", err)
	}
	if len(afterDelete.GetRecordsJson()) != 0 {
		t.Fatalf("deleted row is still selectable: %s", string(afterDelete.GetRecordsJson()[0]))
	}
}

func liveRequestContext(tenant, project, purpose string) *entityv1.RequestContext {
	return &entityv1.RequestContext{
		TenantId:             tenant,
		ProjectId:            project,
		Purpose:              purpose,
		CorrelationId:        purpose + "-" + time.Now().UTC().Format("20060102150405"),
		// Scopes are NOT client-asserted in the request body — the broker
		// authorizes from the validated bearer JWT, not RequestContext.Scopes.
		ServiceIdentity:      "go.sdk.live",
		ClientCatalogVersion: ProtocolVersion,
	}
}

func liveStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	out, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("build Struct: %v", err)
	}
	return out
}

func liveRecordJSON(t *testing.T, recordID, tenant, project, lookupKey, payload string, revision int64) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"record_id":  recordID,
		"tenant_id":  tenant,
		"project_id": project,
		"lookup_key": lookupKey,
		"payload":    payload,
		"revision":   revision,
	})
	if err != nil {
		t.Fatalf("marshal live record: %v", err)
	}
	return raw
}

func liveMutationPayload(t *testing.T, response *entityv1.MutationResponse) string {
	t.Helper()
	var row map[string]any
	if err := json.Unmarshal(response.GetRecordJson(), &row); err != nil {
		t.Fatalf("decode mutation record_json: %v", err)
	}
	value, _ := row["payload"].(string)
	return value
}

func liveRecordPayload(t *testing.T, records *entityv1.RecordSet, index int) string {
	t.Helper()
	raw := records.GetRecordsJson()
	if len(raw) <= index {
		t.Fatalf("RecordSet.records_json[%d] missing", index)
	}
	var row map[string]any
	if err := json.Unmarshal(raw[index], &row); err != nil {
		t.Fatalf("decode record_json: %v", err)
	}
	value, _ := row["payload"].(string)
	return value
}

func liveDocPayload(set *entityv1.DocumentSet) string {
	if len(set.GetDocuments()) == 0 {
		return ""
	}
	return set.GetDocuments()[0].GetFields()["payload"].GetStringValue()
}

func containsResource(resources []string, needle string) bool {
	for _, resource := range resources {
		if strings.Contains(resource, needle) {
			return true
		}
	}
	return false
}

func liveGeneratedOptions(meta Metadata, authz string) Options {
	return Options{
		Meta:          meta,
		Authorization: authz,
		CallTimeout:   2 * time.Second,
		Retry: RetryConfig{
			MaxAttempts:  1,
			BaseBackoff:  1 * time.Millisecond,
			MaxBackoff:   1 * time.Millisecond,
			RetryOnCodes: []codes.Code{},
		},
	}
}

func probeLiveRPC(ctx context.Context, gen *GeneratedClient, rpc RPCInfo) error {
	switch rpc.Kind {
	case KindUnary:
		return gen.InvokeUnary(ctx, rpc.FullMethod, &emptypb.Empty{}, &emptypb.Empty{})
	case KindServerStreaming:
		stream, err := gen.NewServerStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ServerStreams: true}, &emptypb.Empty{})
		if err != nil {
			return err
		}
		return stream.RecvMsg(&emptypb.Empty{})
	case KindClientStreaming:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		return stream.RecvMsg(&emptypb.Empty{})
	case KindBidi:
		stream, err := gen.NewClientStream(ctx, rpc.FullMethod, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
		if err != nil {
			return err
		}
		if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		_ = stream.CloseSend()
		return stream.RecvMsg(&emptypb.Empty{})
	default:
		return nil
	}
}

func isLiveMountFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	// Unimplemented = not wired; Unavailable = listener/backend not serving it;
	// Unknown = no usable status (likely an unmounted path). DeadlineExceeded is
	// NOT a mount failure: an unmounted RPC returns Unimplemented instantly, so a
	// timeout means the server accepted the call and is processing/blocking — e.g.
	// PublishCDC is an open-ended CDC *subscription* stream that legitimately
	// blocks waiting for events, which is proof it reached the live implementation.
	case codes.Unimplemented, codes.Unavailable, codes.Unknown:
		return true
	default:
		return false
	}
}

func assertRequiredLiveBackends(t *testing.T, enabled []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, backend := range enabled {
		seen[strings.ToLower(backend)] = true
	}
	for _, backend := range strings.Split(liveEnv("UDB_LIVE_REQUIRED_BACKENDS", "postgres,mongodb,minio"), ",") {
		backend = strings.ToLower(strings.TrimSpace(backend))
		if backend == "" {
			continue
		}
		if !seen[backend] {
			t.Fatalf("GetCapabilities enabled_backends=%v, missing required backend %q", enabled, backend)
		}
	}
}

func requiredLiveEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required when UDB_LIVE_SDK_TESTS=1", name)
	}
	return value
}

func liveEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
