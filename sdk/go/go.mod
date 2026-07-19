// Go module for the UDB Go SDK. Lives in a sub-path of the
// standalone UDB repo so `go get github.com/fahara02/udb/sdk/go@vX`
// pulls just the SDK without compiling the Rust runtime tree.
//
// Generated stubs land under `./gen/...` via `buf generate`
// (configured in the repo-root buf.gen.yaml with
// `go_package_prefix: github.com/fahara02/udb/sdk/go/gen`).
// Consumers don't need buf installed — committed gen/ files mean
// `go get` works out of the box.
module github.com/fahara02/udb/sdk/go

go 1.22

require (
	google.golang.org/genproto/googleapis/api v0.0.0-20240814211410-ddb44dafa142
	google.golang.org/grpc v1.67.1
	google.golang.org/protobuf v1.35.1
)

require (
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240814211410-ddb44dafa142 // indirect
)
