package main

import (
	"context"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	storageeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/events/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"log"
	"os"
)

func main() {
	u, err := udbclient.ConnectFromEnv(context.Background(), "UDB_", udbclient.Expect{})
	if err != nil {
		log.Fatal(err)
	}
	defer u.Close()
	file := &storagev1.File{FileId: os.Getenv("EXAMPLE_FILE_ID"), Filename: "after.txt", ObjectKey: "go_service/" + os.Getenv("EXAMPLE_FILE_ID"), Status: storagev1.FileStatus_FILE_STATUS_PENDING}
	if err := udbclient.TableOf[*storagev1.File](u).Upsert(context.Background(), &storagev1.File{FileId: file.FileId, Filename: "before.txt", ObjectKey: file.ObjectKey, Status: file.Status}); err != nil {
		log.Fatal(err)
	}
	if err := u.Tx(context.Background(), func(tx *udbclient.TxScope) error {
		tx.Emit(&storageeventsv1.FileMetadataUpdated{FileId: file.FileId, TenantId: u.Meta.TenantID, Filename: file.Filename})
		return tx.UpdateIf(file, udbclient.Record{"filename": "before.txt"})
	}); err != nil {
		log.Fatal(err)
	}
	log.Fatal(udbclient.Consume(context.Background(), u, "go-service-"+file.FileId, udbclient.Topic(&storageeventsv1.FileMetadataUpdated{}), func(_ context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileMetadataUpdated]) error {
		log.Printf("handled %s %s", event.Payload.GetFileId(), event.ID)
		return nil
	}, udbclient.ConsumeOptions{FromEarliest: true}))
}
