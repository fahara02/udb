package udbclient

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	"google.golang.org/grpc"
)

// recordingStorageClient records the ordered sequence of storage RPCs invoked so
// the test can assert UploadFile issues EXACTLY [RegisterUpload, FinalizeUpload]
// (the PUT is recorded separately by the injected httpDoer). Any other RPC
// (GetFile/ListFiles) appends to the sequence and fails the guardrail.
type recordingStorageClient struct {
	storagev1.StorageServiceClient
	seq         *[]string
	uploadURL   string
	lastFinSize int64
	lastFinFile string
}

func (c *recordingStorageClient) RegisterUpload(_ context.Context, _ *storagev1.RegisterUploadRequest, _ ...grpc.CallOption) (*storagev1.RegisterUploadResponse, error) {
	*c.seq = append(*c.seq, "RegisterUpload")
	return &storagev1.RegisterUploadResponse{FileId: "file-xyz", UploadUrl: c.uploadURL}, nil
}

func (c *recordingStorageClient) FinalizeUpload(_ context.Context, in *storagev1.FinalizeUploadRequest, _ ...grpc.CallOption) (*storagev1.FinalizeUploadResponse, error) {
	*c.seq = append(*c.seq, "FinalizeUpload")
	c.lastFinFile = in.FileId
	c.lastFinSize = in.SizeBytes
	return &storagev1.FinalizeUploadResponse{}, nil
}

func (c *recordingStorageClient) GetFile(_ context.Context, _ *storagev1.GetFileRequest, _ ...grpc.CallOption) (*storagev1.GetFileResponse, error) {
	*c.seq = append(*c.seq, "GetFile")
	return &storagev1.GetFileResponse{}, nil
}

func (c *recordingStorageClient) ListFiles(_ context.Context, _ *storagev1.ListFilesRequest, _ ...grpc.CallOption) (*storagev1.ListFilesResponse, error) {
	*c.seq = append(*c.seq, "ListFiles")
	return &storagev1.ListFilesResponse{}, nil
}

// recordingDoer records the PUT into the shared sequence.
type recordingDoer struct {
	seq *[]string
	got *http.Request
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	*d.seq = append(*d.seq, req.Method) // "PUT"
	d.got = req
	return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
}

func withUploadDoer(t *testing.T, d httpDoer) func() {
	t.Helper()
	prev := uploadHTTPDoer
	uploadHTTPDoer = d
	return func() { uploadHTTPDoer = prev }
}

func TestUploadFileExactSequence(t *testing.T) {
	var seq []string
	fakeStore := &recordingStorageClient{seq: &seq, uploadURL: "https://obj.example/put/file-xyz"}
	doer := &recordingDoer{seq: &seq}
	defer withUploadDoer(t, doer)()

	f := &StorageFacade{Raw: fakeStore, meta: Metadata{TenantID: "t-1", ProjectID: "p-1"}}
	resp, err := f.UploadFile(context.Background(), "a.png", []byte("hello world"),
		WithContentType("image/png"), WithFileType("image"), WithChecksum("sha256:abc"))
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if resp == nil {
		t.Fatal("nil FinalizeUploadResponse")
	}
	want := loadWorkflowSequence(t, "StorageFacade.uploadFile")
	if !reflect.DeepEqual(seq, want) {
		t.Fatalf("RPC sequence drift: got %v, want %v (no hidden Get/List allowed)", seq, want)
	}
	if fakeStore.lastFinFile != "file-xyz" || fakeStore.lastFinSize != int64(len("hello world")) {
		t.Fatalf("finalize threaded wrong file/size: %q %d", fakeStore.lastFinFile, fakeStore.lastFinSize)
	}
	if doer.got.Header.Get("Content-Type") != "image/png" {
		t.Fatalf("PUT missing Content-Type: %q", doer.got.Header.Get("Content-Type"))
	}
}

func TestUploadFileNoURLSkipsPut(t *testing.T) {
	var seq []string
	fakeStore := &recordingStorageClient{seq: &seq, uploadURL: ""} // broker returned no presigned URL
	doer := &recordingDoer{seq: &seq}
	defer withUploadDoer(t, doer)()

	f := &StorageFacade{Raw: fakeStore, meta: Metadata{TenantID: "t-1"}}
	if _, err := f.UploadFile(context.Background(), "a.txt", []byte("x")); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	want := loadWorkflowSequence(t, "StorageFacade.uploadFile.noUrl") // no PUT
	if !reflect.DeepEqual(seq, want) {
		t.Fatalf("sequence drift with no URL: got %v, want %v", seq, want)
	}
}

func TestUploadFileSizeCap(t *testing.T) {
	var seq []string
	fakeStore := &recordingStorageClient{seq: &seq}
	defer withUploadDoer(t, &recordingDoer{seq: &seq})()

	f := &StorageFacade{Raw: fakeStore, meta: Metadata{TenantID: "t-1"}, MaxUploadBytes: 4}
	if _, err := f.UploadFile(context.Background(), "big.bin", []byte("12345")); err == nil {
		t.Fatal("expected size-cap error")
	}
	if len(seq) != 0 {
		t.Fatalf("size cap must reject before any RPC; got %v", seq)
	}
}

var _ = io.Discard
