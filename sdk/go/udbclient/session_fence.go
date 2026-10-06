package udbclient

import (
	"sync"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
)

// Read-your-writes for typed stores.
//
// After a Table write, the next reads through the same client carry a read
// fence built from that write's receipt, so they see the write even when they
// are served by a replica or a projection. Callers used to build these fences
// by hand (or force every read to the primary); the client now does it for the
// window right after a write.

// readYourWritesWindow is how long after a write its fence rides on reads.
const readYourWritesWindow = 30 * time.Second

// readFenceWaitMs is how long a fenced read may wait for its write to land.
const readFenceWaitMs = 2000

type writeFence struct {
	mu      sync.Mutex
	receipt WriteReceipt
	at      time.Time
}

// rememberWrite keeps the latest write's receipt for the following reads.
func (u *Udb) rememberWrite(res *entityv1.MutationResponse) {
	receipt, err := ReceiptFromMutation(res)
	if err != nil {
		return
	}
	u.rememberReceiptValue(receipt)
}

func (u *Udb) rememberReceiptValue(receipt WriteReceipt) {
	if receipt.IsEmpty() {
		return
	}
	u.fence.mu.Lock()
	u.fence.receipt = receipt
	u.fence.at = time.Now()
	u.fence.mu.Unlock()
}

// readFence is the per-request context carrying the fence of the latest write,
// or nil when there was no write in the window.
func (u *Udb) readFence() *entityv1.RequestContext {
	u.fence.mu.Lock()
	receipt, at := u.fence.receipt, u.fence.at
	u.fence.mu.Unlock()
	if receipt.IsEmpty() || time.Since(at) > readYourWritesWindow {
		return nil
	}
	rc := &entityv1.RequestContext{}
	AfterWrite(rc, receipt, readFenceWaitMs)
	return rc
}
