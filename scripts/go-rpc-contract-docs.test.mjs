// Tests for the generated Go Client method docs (go-rpc-contract-docs.mjs).
// They run against the committed inputs: the native contract, the generated
// CredentialType enum and every committed *_grpc.pb.go, so a contract change
// that was not regenerated into the Go SDK fails here.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { GoRPCContractDocs, credentialTypeNames, methodContractLines } from "./go-rpc-contract-docs.mjs";

const root = new URL("../", import.meta.url);
const read = (relative) => fs.readFileSync(new URL(relative, root), "utf8");
const manifest = () => JSON.parse(read("docs/generated/udb-native-contract.json"));
const names = credentialTypeNames(read("sdk/go/gen/udb/core/common/v1/security.pb.go"));

function grpcFiles(dir = new URL("sdk/go/gen/udb/", root)) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const next = new URL(entry.name + (entry.isDirectory() ? "/" : ""), dir);
    if (entry.isDirectory()) out.push(...grpcFiles(next));
    else if (entry.name.endsWith("_grpc.pb.go")) out.push(next);
  }
  return out;
}

const contractBlock = /^\t\/\/ UDB contract: [^\n]+\n(?:\t\/\/[^\n]*\n)*?\t\/\/ End UDB contract\.\n/gmu;

test("committed Go clients carry exactly the docs the contract produces, for every canonical method", () => {
  const docs = new GoRPCContractDocs(manifest(), names);
  for (const file of grpcFiles()) {
    const committed = fs.readFileSync(file, "utf8").replace(/\r\n/gu, "\n");
    const rel = path.relative(new URL(".", root).pathname, file.pathname);
    assert.equal(docs.annotate(committed, rel), committed, `${rel} is stale: run node scripts/sdk-codegen-postprocess.mjs`);
  }
  assert.equal(docs.finish(), manifest().services.reduce((n, s) => n + s.rpcs.length, 0));
});

test("re-annotation replaces its own blocks, never accumulates them, and keeps proto comments", () => {
  const docs = new GoRPCContractDocs(manifest(), names);
  const [file] = grpcFiles().filter((f) => f.pathname.endsWith("data_broker_grpc.pb.go"));
  const committed = fs.readFileSync(file, "utf8").replace(/\r\n/gu, "\n");
  const bare = committed.replace(contractBlock, "");
  const once = docs.annotate(bare, "data_broker_grpc.pb.go");
  assert.equal(once, committed);
  const again = new GoRPCContractDocs(manifest(), names).annotate(once, "data_broker_grpc.pb.go");
  assert.equal(again, once);
  for (const line of bare.split("\n").filter((l) => l.startsWith("\t//"))) {
    assert.ok(once.includes(line), `lost proto comment: ${line}`);
  }
});

test("credential enum names come from the generated map and strip the prefix", () => {
  assert.equal(names.get(1) !== undefined, true);
  for (const name of names.values()) assert.doesNotMatch(name, /^CREDENTIAL_TYPE_/u);
  const parsed = credentialTypeNames(`
var (
	CredentialType_name = map[int32]string{
		0: "CREDENTIAL_TYPE_UNSPECIFIED",
		1: "CREDENTIAL_TYPE_BEARER_JWT",
		2: "CREDENTIAL_TYPE_SESSION",
	}
)`);
  assert.deepEqual([...parsed], [[0, "UNSPECIFIED"], [1, "BEARER_JWT"], [2, "SESSION"]]);
  assert.throws(() => credentialTypeNames("package commonv1"), /CredentialType_name enum is missing/u);
  assert.throws(() => credentialTypeNames("CredentialType_name = map[int32]string{\n\t}"), /enum is empty/u);
});

test("method docs state listener, scopes, credentials, kind and the replay contract", () => {
  const { services } = manifest();
  const find = (predicate) => {
    for (const service of services) for (const rpc of service.rpcs) if (predicate(rpc, service)) return { service, rpc };
    throw new Error("no canonical method matches");
  };
  const beginTx = find((rpc) => rpc.path === "/udb.services.v1.DataBroker/BeginTx");
  const lines = methodContractLines(beginTx.service, beginTx.rpc, names);
  assert.equal(lines[0], "UDB contract: /udb.services.v1.DataBroker/BeginTx");
  assert.equal(lines.at(-1), "End UDB contract.");
  assert.ok(lines.some((l) => l.includes("upsert, update, delete and vector_upsert")));
  assert.ok(lines.some((l) => l.startsWith("Relational mutation keys use durable replay receipts")));

  const readOnly = find((rpc) => rpc.kind === "unary" && rpc.operation_kind === "read_only");
  assert.ok(methodContractLines(readOnly.service, readOnly.rpc, names).includes(
    "Idempotency: read-only unary calls may retry transient failures."));

  const stream = find((rpc) => rpc.kind !== "unary");
  assert.ok(methodContractLines(stream.service, stream.rpc, names).includes(
    "Idempotency: streams are not automatically replayed; use the RPC resume contract."));
});

test("incomplete or inconsistent contract metadata refuses generation", () => {
  const unknownCredential = manifest();
  const target = unknownCredential.services.flatMap((s) => s.rpcs.map((rpc) => ({ s, rpc })))
    .find(({ rpc }) => (rpc.endpoint_security?.allowed_credential_types ?? []).length);
  target.rpc.endpoint_security.allowed_credential_types = [9999];
  assert.throws(() => new GoRPCContractDocs(unknownCredential, names), /unknown credential type 9999/u);

  const badCount = manifest();
  badCount.service_count += 1;
  assert.throws(() => new GoRPCContractDocs(badCount, names), /service_count does not match/u);

  const duplicate = manifest();
  duplicate.services[0].rpcs.push(structuredClone(duplicate.services[0].rpcs[0]));
  duplicate.services[0].rpc_count += 1;
  assert.throws(() => new GoRPCContractDocs(duplicate, names), /duplicate canonical method/u);

  const badKind = manifest();
  badKind.services[0].rpcs[0].operation_kind = "sometimes";
  assert.throws(() => new GoRPCContractDocs(badKind, names), /invalid operation kind/u);
});

test("a generated method missing from the contract, or never generated, fails", () => {
  const docs = new GoRPCContractDocs(manifest(), names);
  const source = [
    'const (\n\tDataBroker_Ghost_FullMethodName = "/udb.services.v1.DataBroker/Ghost"\n)',
    "type DataBrokerClient interface {",
    "\tGhost(ctx context.Context, in *Req, opts ...grpc.CallOption) (*Resp, error)",
    "}",
  ].join("\n");
  assert.throws(() => docs.annotate(source, "ghost_grpc.pb.go"), /no canonical contract: \/udb.services.v1.DataBroker\/Ghost/u);
  assert.throws(() => new GoRPCContractDocs(manifest(), names).finish(), /canonical methods missing generated Client docs/u);
});
