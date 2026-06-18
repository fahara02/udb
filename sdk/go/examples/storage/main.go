package main

import (
	"bytes"
	"context"
	"log"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
)

func main() {
	ctx := context.Background()

	// Dial the broker once and wire the full project facade. Storage is served on
	// the native control-plane listener, so it reuses the auth connection.
	udb, err := udbclient.NewUdb(ctx, udbclient.Config{
		Target:          "localhost:50051",
		Purpose:         "media-upload",
		CorrelationID:   "go-storage-example",
		ServiceIdentity: "example.service",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer udb.Close()

	// Log in and adopt the verified principal's canonical tenant/project across
	// every facade; the bearer token is installed as the call credential.
	if _, err := udb.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{
		Username: "admin",
		Password: "admin",
	}); err != nil {
		log.Fatal(err)
	}

	// Upload bytes: RegisterUpload -> presigned PUT -> FinalizeUpload.
	payload := []byte("hello from the udb go sdk")
	up, err := udb.Storage.UploadFile(ctx, "greeting.txt", payload,
		udbclient.WithContentType("text/plain"))
	if err != nil {
		log.Fatal(err)
	}
	fileID := up.GetFile().GetFileId()
	log.Printf("uploaded file_id=%s size=%d", fileID, len(payload))

	// Presigned-URL download is the happy path (bytes never transit the broker):
	//   url, err := udb.Storage.DownloadFile(ctx, fileID, 15) // 15-minute URL
	// Here we use the 0.3.6 server-streaming fallback to read the bytes back
	// through the broker when a presigned HTTP URL cannot be used.
	res, err := udb.Storage.DownloadFileBytes(ctx, fileID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("downloaded %d bytes content_type=%s", len(res.Data), res.ContentType)

	if !bytes.Equal(res.Data, payload) {
		log.Fatalf("round-trip mismatch: got %q want %q", res.Data, payload)
	}
	log.Print("round-trip OK")
}
