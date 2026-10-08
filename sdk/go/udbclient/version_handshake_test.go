package udbclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A transport fixture, with no database/store substitute: compatibility is
// tested across an actual gRPC listener, including generated typed bindings.
type versionHandshakeBroker struct {
	servicesv1.UnimplementedDataBrokerServer
	t        *testing.T
	versions []string
	rpcError error
	detail   []byte
	calls    atomic.Int32
}

type versionHandshakeAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	broker *versionHandshakeBroker
}

func (a *versionHandshakeAuthn) Login(ctx context.Context, _ *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	a.broker.headers(ctx)
	return &authnv1.LoginResponse{AccessToken: "test-version-handshake-token"}, nil
}

func (b *versionHandshakeBroker) headers(ctx context.Context) {
	b.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("x-udb-sdk-version"); len(got) != 1 || got[0] != SDKVersion {
		b.t.Errorf("served SDK release header = %v; want [%s]", got, SDKVersion)
	}
	if got := md.Get("x-test-caller"); len(got) != 1 || got[0] != "preserved" {
		b.t.Errorf("caller metadata was lost: %v", got)
	}
	header := metadata.Pairs("x-test-header", "response-header")
	for _, version := range b.versions {
		header.Append("x-udb-version", version)
	}
	if err := grpc.SendHeader(ctx, header); err != nil {
		b.t.Errorf("send response metadata: %v", err)
	}
	trailer := metadata.Pairs("x-test-trailer", "response-trailer")
	if b.detail != nil {
		trailer.Set(errorDetailTrailer, string(b.detail))
	}
	grpc.SetTrailer(ctx, trailer)
}

func (b *versionHandshakeBroker) Select(ctx context.Context, _ *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	b.headers(ctx)
	return &entityv1.RecordSet{}, b.rpcError
}

func (b *versionHandshakeBroker) PublishCDC(_ *entityv1.CDCSubscriptionRequest, stream grpc.ServerStreamingServer[eventsv1.CDCEnvelope]) error {
	b.headers(stream.Context())
	return stream.Send(&eventsv1.CDCEnvelope{})
}

func (b *versionHandshakeBroker) BatchUpsert(stream grpc.BidiStreamingServer[entityv1.UpsertRequest, entityv1.MutationResponse]) error {
	// Sending the first request must remain possible before the client waits
	// for our version header; eager Header() in NewStream would deadlock here.
	if _, err := stream.Recv(); err != nil {
		return err
	}
	b.headers(stream.Context())
	return stream.Send(&entityv1.MutationResponse{})
}

func (b *versionHandshakeBroker) PutObject(stream grpc.ClientStreamingServer[entityv1.Chunk, entityv1.MutationResponse]) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	b.headers(stream.Context())
	return stream.SendAndClose(&entityv1.MutationResponse{})
}

func versionTestConnection(t *testing.T, broker *versionHandshakeBroker, opt Options, intercepted bool) (*GeneratedClient, *grpc.ClientConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	servicesv1.RegisterDataBrokerServer(server, broker)
	authnv1.RegisterAuthnServiceServer(server, &versionHandshakeAuthn{broker: broker})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	g := NewGenerated(nil, opt)
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if intercepted {
		dialOpts = append(dialOpts, g.DialOptions()...)
	}
	conn, err := grpc.NewClient(listener.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	g.rebindConn(conn)
	return g, conn
}

func versionTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-test-caller", "preserved",
		"x-udb-sdk-version", "stale-caller-version",
		"x-udb-sdk-version", "duplicate-caller-version",
	))
}

func incompatibleTestVersion(t *testing.T) string {
	t.Helper()
	parts := strings.Split(SDKVersion, ".")
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return parts[0] + "." + strconv.Itoa(minor+1) + ".0"
}

func TestVersionHandshakeUnaryWire(t *testing.T) {
	parts := strings.Split(SDKVersion, ".")
	patch := parts[0] + "." + parts[1] + ".999-rc.1+ci"
	for _, path := range []string{"generated", "typed", "generated-with-interceptor"} {
		for _, test := range []struct {
			name     string
			versions []string
			strict   bool
			mismatch bool
		}{
			{"same-release", []string{SDKVersion}, true, false},
			{"same-minor-patch", []string{patch}, true, false},
			{"warn-different-minor", []string{incompatibleTestVersion(t)}, false, true},
			{"strict-different-minor", []string{incompatibleTestVersion(t)}, true, true},
			{"strict-different-major", []string{"999.0.0"}, true, true},
			{"strict-missing", nil, true, true},
			{"warn-missing", nil, false, true},
			{"strict-malformed", []string{"0.5"}, true, true},
			{"strict-invalid-prerelease", []string{parts[0] + "." + parts[1] + ".0-01"}, true, true},
			{"strict-duplicate", []string{SDKVersion, SDKVersion}, true, true},
		} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				broker := &versionHandshakeBroker{t: t, versions: test.versions}
				var warnings atomic.Int32
				g, conn := versionTestConnection(t, broker, Options{
					Retry: fastRetry(), StrictServerVersion: test.strict,
					OnVersionWarning: func(error) { warnings.Add(1) },
				}, path != "generated")
				ctx := versionTestContext(t)
				for call := 0; call < 2; call++ {
					var h1, h2, tr1, tr2 metadata.MD
					opts := []grpc.CallOption{grpc.Header(&h1), grpc.Trailer(&tr1), grpc.Header(&h2), grpc.Trailer(&tr2)}
					var err error
					if path == "typed" {
						_, err = servicesv1.NewDataBrokerClient(conn).Select(ctx, &entityv1.SelectRequest{}, opts...)
					} else {
						err = g.InvokeUnary(ctx, servicesv1.DataBroker_Select_FullMethodName, &entityv1.SelectRequest{}, &entityv1.RecordSet{}, opts...)
					}
					var mismatch *VersionMismatchError
					if test.strict && test.mismatch {
						if !errors.As(err, &mismatch) {
							t.Fatalf("strict mismatch = %v, want VersionMismatchError", err)
						}
					} else if err != nil {
						t.Fatalf("compatible/default-warning call: %v", err)
					}
					for _, h := range []metadata.MD{h1, h2} {
						if got := h.Get("x-test-header"); len(got) != 1 || got[0] != "response-header" {
							t.Fatalf("caller response header missing: %v", h)
						}
					}
					for _, trailer := range []metadata.MD{tr1, tr2} {
						if got := trailer.Get("x-test-trailer"); len(got) != 1 || got[0] != "response-trailer" {
							t.Fatalf("caller response trailer missing: %v", trailer)
						}
					}
				}
				if got := broker.calls.Load(); got != 2 {
					t.Fatalf("version mismatch caused retries: %d served calls, want 2", got)
				}
				wantWarnings := int32(0)
				if test.mismatch && !test.strict {
					wantWarnings = 1
				}
				if got := warnings.Load(); got != wantWarnings {
					t.Fatalf("warnings = %d, want %d", got, wantWarnings)
				}
			})
		}
	}
}

func TestVersionHandshakePreservesTypedErrorAndCallerTrailers(t *testing.T) {
	detail, err := proto.Marshal(&entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION, Operation: "select"})
	if err != nil {
		t.Fatal(err)
	}
	for _, intercepted := range []bool{false, true} {
		broker := &versionHandshakeBroker{t: t, versions: []string{incompatibleTestVersion(t)}, rpcError: status.Error(codes.InvalidArgument, "served validation refusal"), detail: detail}
		g, _ := versionTestConnection(t, broker, Options{Retry: fastRetry(), StrictServerVersion: true}, intercepted)
		var trailer metadata.MD
		err := g.InvokeUnary(versionTestContext(t), servicesv1.DataBroker_Select_FullMethodName, &entityv1.SelectRequest{}, &entityv1.RecordSet{}, grpc.Trailer(&trailer))
		typed, ok := AsError(err)
		if !ok || typed.Code != codes.InvalidArgument || !bytes.Equal(typed.DetailBin, detail) {
			t.Fatalf("typed server refusal changed: %v", err)
		}
		if got := trailer.Get(errorDetailTrailer); len(got) != 1 || !bytes.Equal([]byte(got[0]), detail) {
			t.Fatalf("caller binary trailer was lost: %v", trailer)
		}
		if got := broker.calls.Load(); got != 1 {
			t.Fatalf("server refusal retried: %d", got)
		}
	}
}

func TestVersionHandshakeFrontDoorOptions(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run("NewUdb/strict="+strconv.FormatBool(strict), func(t *testing.T) {
			broker := &versionHandshakeBroker{t: t, versions: []string{incompatibleTestVersion(t)}}
			_, fixtureConn := versionTestConnection(t, broker, Options{}, false)
			var warnings atomic.Int32
			u, err := NewUdb(versionTestContext(t), Config{
				Target: fixtureConn.Target(), StrictServerVersion: strict,
				OnVersionWarning: func(error) { warnings.Add(1) },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = u.Close() })
			_, err = u.Data.Broker.Select(versionTestContext(t), &entityv1.SelectRequest{})
			var mismatch *VersionMismatchError
			if strict {
				if !errors.As(err, &mismatch) {
					t.Fatalf("front-door strict mismatch = %v", err)
				}
			} else if err != nil || warnings.Load() != 1 {
				t.Fatalf("front-door warning policy: err=%v warnings=%d", err, warnings.Load())
			}
			if broker.calls.Load() != 1 {
				t.Fatalf("front-door version refusal retried: %d", broker.calls.Load())
			}
		})
	}
	t.Run("ConnectEnterprise/strict", func(t *testing.T) {
		broker := &versionHandshakeBroker{t: t, versions: []string{incompatibleTestVersion(t)}}
		_, fixtureConn := versionTestConnection(t, broker, Options{}, false)
		session, err := ConnectEnterprise(versionTestContext(t), EnterpriseConfig{
			Target: fixtureConn.Target(), Username: "version-test", Password: "version-test",
			StrictServerVersion: true,
		})
		if session != nil {
			_ = session.Close()
			t.Fatal("strict incompatible login was adopted")
		}
		var mismatch *VersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("enterprise strict login mismatch = %v", err)
		}
		if broker.calls.Load() != 1 {
			t.Fatalf("incompatible login retried: %d", broker.calls.Load())
		}
	})
}

func TestVersionHandshakeStreamWire(t *testing.T) {
	for _, intercepted := range []bool{false, true} {
		for _, mismatch := range []bool{false, true} {
			for _, kind := range []string{"server", "bidi", "client"} {
				t.Run(strconv.FormatBool(intercepted)+"/"+strconv.FormatBool(mismatch)+"/"+kind, func(t *testing.T) {
					version := SDKVersion
					if mismatch {
						version = incompatibleTestVersion(t)
					}
					broker := &versionHandshakeBroker{t: t, versions: []string{version}}
					g, conn := versionTestConnection(t, broker, Options{StrictServerVersion: true, CallTimeout: time.Second}, intercepted)
					ctx := versionTestContext(t)
					var stream grpc.ClientStream
					var err error
					var response any = &entityv1.MutationResponse{}
					switch kind {
					case "server":
						response = &eventsv1.CDCEnvelope{}
						if intercepted {
							stream, err = servicesv1.NewDataBrokerClient(conn).PublishCDC(ctx, &entityv1.CDCSubscriptionRequest{})
						} else {
							stream, err = g.NewServerStream(ctx, servicesv1.DataBroker_PublishCDC_FullMethodName, &grpc.StreamDesc{ServerStreams: true}, &entityv1.CDCSubscriptionRequest{})
						}
					case "bidi":
						if intercepted {
							stream, err = servicesv1.NewDataBrokerClient(conn).BatchUpsert(ctx)
						} else {
							stream, err = g.NewClientStream(ctx, servicesv1.DataBroker_BatchUpsert_FullMethodName, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true})
						}
					case "client":
						if intercepted {
							stream, err = servicesv1.NewDataBrokerClient(conn).PutObject(ctx)
						} else {
							stream, err = g.NewClientStream(ctx, servicesv1.DataBroker_PutObject_FullMethodName, &grpc.StreamDesc{ClientStreams: true})
						}
					}
					if err != nil {
						t.Fatalf("open stream: %v", err)
					}
					if kind != "server" {
						var request any = &entityv1.UpsertRequest{}
						if kind == "client" {
							request = &entityv1.Chunk{}
						}
						if err := stream.SendMsg(request); err != nil {
							t.Fatalf("send first request before version check: %v", err)
						}
						if err := stream.CloseSend(); err != nil {
							t.Fatal(err)
						}
					}
					err = stream.RecvMsg(response)
					var versionErr *VersionMismatchError
					if mismatch {
						if !errors.As(err, &versionErr) {
							t.Fatalf("stream mismatch: %v", err)
						}
					} else if err != nil {
						t.Fatalf("compatible stream: %v", err)
					}
					if broker.calls.Load() != 1 {
						t.Fatalf("stream retried: %d", broker.calls.Load())
					}
				})
			}
		}
	}
}
