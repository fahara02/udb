// Facade unit tests (Phase 7 M8). Pure unit tests with no live server: a
// capturing fake `UdbCore` records the exact (serviceFull, method, request) each
// wrapper emits, so we assert the storage / asset / webrtc facades route to the
// right RPC and build the right request. Run with Node's built-in runner over
// compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { readFileSync, readdirSync } from "node:fs";
import * as path from "node:path";

import { EntityHandle } from "./entity";
import { UdbCore } from "./generatedClient";
import {
  ApiKeyFacade,
  AssetFacade,
  AuthzFacade,
  StorageFacade,
  UdbProject,
  WebRtcFacade,
} from "./project";

interface UnaryCall {
  service: string;
  method: string;
  request: any;
}

// A capturing fake UdbCore: skips the real proto loader / gRPC channel and just
// records each invoker call. Only the surfaces the facades touch are populated.
function fakeCore(): { core: UdbCore; unary: UnaryCall[]; streams: UnaryCall[] } {
  const unary: UnaryCall[] = [];
  const streams: UnaryCall[] = [];
  const core: any = Object.create(UdbCore.prototype);
  core.unary = (service: string, method: string, request: any) => {
    unary.push({ service, method, request });
    return Promise.resolve({ ok: true });
  };
  core.serverStream = (service: string, method: string, request: any) => {
    streams.push({ service, method, request });
    return { kind: "serverStream" } as any;
  };
  core.clientStream = (service: string, method: string) => {
    streams.push({ service, method, request: undefined });
    return { stream: {}, response: Promise.resolve({}) } as any;
  };
  core.bidiStream = (service: string, method: string) => {
    streams.push({ service, method, request: undefined });
    return { kind: "duplex" } as any;
  };
  return { core: core as UdbCore, unary, streams };
}

const STORAGE = "udb.core.storage.services.v1.StorageService";
const DATABROKER = "udb.services.v1.DataBroker";
const ASSET = "udb.core.asset.services.v1.AssetService";
const APIKEY = "udb.core.apikey.services.v1.ApiKeyService";

// ── shared workflow-sequence fixture (chapter 11.2.x) ─────────────────────────
// docs/bench-bodies/workflow-sequences.md is the cross-SDK SINGLE SOURCE OF TRUTH
// for the exact ordered RPC sequence each workflow helper may emit (`| helper |
// sequence |` table, col2 = comma-separated ordered method names; the literal
// token `PUT` = the presigned-URL byte transfer, not a gRPC call). The TS / Go /
// Python mock-transport gates each read THIS file instead of carrying an inline
// list, so a drift in the contract fails every language identically.
function workflowSequencesPath(): string {
  const candidates = [
    path.resolve(__dirname, "../../../docs/bench-bodies/workflow-sequences.md"), // dev: dist-test/
    path.resolve(__dirname, "../../docs/bench-bodies/workflow-sequences.md"),
    path.resolve(__dirname, "../docs/bench-bodies/workflow-sequences.md"),
  ];
  for (const c of candidates) {
    try {
      readdirSync(path.dirname(c));
      readFileSync(c, "utf8");
      return c;
    } catch {
      /* not this candidate */
    }
  }
  return candidates[0];
}

function loadWorkflowSequence(helper: string): string[] {
  const text = readFileSync(workflowSequencesPath(), "utf8");
  for (const line of text.split(/\r?\n/)) {
    const cells = line.split("|").map((c) => c.trim());
    // Data row: `| helper | sequence |` → ["", helper, sequence, ""].
    if (cells.length < 4) continue;
    if (cells[1] !== helper) continue;
    return cells[2].split(",").map((m) => m.trim()).filter((m) => m.length > 0);
  }
  throw new Error(`workflow-sequences.md has no row for helper "${helper}"`);
}

test("ApiKeyFacade rotates through atomic RotateApiKey RPC", async () => {
  const { core, unary } = fakeCore();
  const apiKey = new ApiKeyFacade(core);

  await apiKey.rotate("key-1", { rotation_reason: "scheduled", context: { tenant_id: "acme" } });

  assert.deepEqual(unary, [
    {
      service: APIKEY,
      method: "RotateApiKey",
      request: {
        key_id: "key-1",
        rotation_reason: "scheduled",
        context: { tenant_id: "acme" },
      },
    },
  ]);
});

test("StorageFacade routes file-lifecycle RPCs to StorageService", async () => {
  const { core, unary } = fakeCore();
  const storage = new StorageFacade(core);

  await storage.registerUpload({ tenant_id: "acme", file_name: "a.png" });
  await storage.finalizeUpload({ tenant_id: "acme", file_id: "f1" });
  await storage.getDownloadUrl("f1");
  await storage.getFile("f1");
  await storage.updateFile({ tenant_id: "acme", file_id: "f1" });
  await storage.deleteFile("f1");
  await storage.listFiles();

  const methods = unary.map((c) => c.method);
  assert.deepEqual(methods, [
    "RegisterUpload",
    "FinalizeUpload",
    "GetDownloadUrl",
    "GetFile",
    "UpdateFile",
    "DeleteFile",
    "ListFiles",
  ]);
  assert.ok(unary.every((c) => c.service === STORAGE));
  // String-id convenience expands to the right field name.
  assert.deepEqual(unary.find((c) => c.method === "GetFile")!.request, { file_id: "f1" });
  assert.deepEqual(unary.find((c) => c.method === "DeleteFile")!.request, { file_id: "f1" });
});

test("StorageFacade retains DataBroker object-byte IO escape hatches", async () => {
  const { core, unary, streams } = fakeCore();
  const storage = new StorageFacade(core);

  storage.putObject();
  storage.getObject({ key: "k" });
  await storage.presign({ key: "k" });

  assert.deepEqual(
    streams.map((c) => [c.service, c.method]),
    [
      [DATABROKER, "PutObject"],
      [DATABROKER, "GetObject"],
    ],
  );
  // presign goes through the DataBroker object surface, not StorageService.
  assert.deepEqual(unary.find((c) => c.method === "GeneratePresignedUrl"), {
    service: DATABROKER,
    method: "GeneratePresignedUrl",
    request: { key: "k" },
  });
});

test("AssetFacade routes pipeline + asset RPCs to AssetService", async () => {
  const { core, unary } = fakeCore();
  const asset = new AssetFacade(core);

  await asset.createPipelineDefinition({ tenant_id: "acme" });
  await asset.getPipelineDefinition("d1");
  await asset.registerAsset({ tenant_id: "acme" });
  await asset.startPipeline({ tenant_id: "acme", asset_id: "a1" });
  await asset.getPipeline("i1");
  await asset.completeStep({ tenant_id: "acme", instance_id: "i1" });
  await asset.listAssets();
  await asset.getAsset("a1");

  const methods = unary.map((c) => c.method);
  assert.deepEqual(methods, [
    "CreatePipelineDefinition",
    "GetPipelineDefinition",
    "RegisterAsset",
    "StartPipeline",
    "GetPipeline",
    "CompleteStep",
    "ListAssets",
    "GetAsset",
  ]);
  assert.ok(unary.every((c) => c.service === ASSET));
  assert.deepEqual(unary.find((c) => c.method === "GetPipelineDefinition")!.request, {
    definition_id: "d1",
  });
  assert.deepEqual(unary.find((c) => c.method === "GetPipeline")!.request, { instance_id: "i1" });
  assert.deepEqual(unary.find((c) => c.method === "GetAsset")!.request, { asset_id: "a1" });
});

test("WebRtcFacade routes to per-service room/peer/track/turn + bidi signal", async () => {
  const { core, unary, streams } = fakeCore();
  const webrtc = new WebRtcFacade(core);

  await webrtc.room.createRoom({ tenant_id: "acme" });
  await webrtc.room.getRoom("r1");
  await webrtc.room.closeRoom("r1");
  await webrtc.peer.joinRoom({ tenant_id: "acme", room_id: "r1" });
  await webrtc.peer.getPeer("p1");
  await webrtc.track.publishTrack({ tenant_id: "acme", peer_id: "p1" });
  await webrtc.track.muteTrack({ tenant_id: "acme", track_id: "t1" });
  await webrtc.turn.issueCredentials({ tenant_id: "acme", room_id: "r1" });

  assert.deepEqual(
    unary.map((c) => [c.service, c.method]),
    [
      ["udb.core.webrtc.services.v1.RoomService", "CreateRoom"],
      ["udb.core.webrtc.services.v1.RoomService", "GetRoom"],
      ["udb.core.webrtc.services.v1.RoomService", "CloseRoom"],
      ["udb.core.webrtc.services.v1.PeerService", "JoinRoom"],
      ["udb.core.webrtc.services.v1.PeerService", "GetPeer"],
      ["udb.core.webrtc.services.v1.TrackService", "PublishTrack"],
      ["udb.core.webrtc.services.v1.TrackService", "MuteTrack"],
      ["udb.core.webrtc.services.v1.TurnService", "IssueCredentials"],
    ],
  );
  assert.deepEqual(unary.find((c) => c.method === "GetRoom")!.request, { room_id: "r1" });
  assert.deepEqual(unary.find((c) => c.method === "GetPeer")!.request, { peer_id: "p1" });

  // The bidi signalling helper opens SignalingService.Signal.
  webrtc.signal();
  assert.deepEqual(streams.map((c) => [c.service, c.method]), [
    ["udb.core.webrtc.services.v1.SignalingService", "Signal"],
  ]);
});

test("UdbProject exposes storage/asset/webrtc facades wired to the shared core", () => {
  // Build a project without the real constructor so no channel/proto load runs;
  // then construct the facades over a fake core exactly as the constructor does.
  const { core } = fakeCore();
  const project: any = Object.create(UdbProject.prototype);
  project.storage = new StorageFacade(core);
  project.asset = new AssetFacade(core);
  project.webrtc = new WebRtcFacade(core);

  assert.ok(project.storage instanceof StorageFacade);
  assert.ok(project.asset instanceof AssetFacade);
  assert.ok(project.webrtc instanceof WebRtcFacade);
  // WebRTC sub-clients are namespaced.
  assert.equal(typeof project.webrtc.room.createRoom, "function");
  assert.equal(typeof project.webrtc.peer.joinRoom, "function");
  assert.equal(typeof project.webrtc.track.publishTrack, "function");
  assert.equal(typeof project.webrtc.turn.issueCredentials, "function");
  assert.equal(typeof project.webrtc.signal, "function");
});

test("StorageFacade.uploadFile = RegisterUpload + PUT + FinalizeUpload (no hidden reads)", async () => {
  // 11.2.2.2: assert against the SHARED workflow-sequences.md fixture, not an
  // inline literal — the gate fails identically across SDKs if the contract drifts.
  const expectedSeq = loadWorkflowSequence("StorageFacade.uploadFile");
  const unary: UnaryCall[] = [];
  // Combined ordered log of EVERY effect (gRPC method names + the `PUT` token at
  // the moment the presigned byte transfer happens) → compared to the full fixture.
  const combined: string[] = [];
  const core: any = Object.create(UdbCore.prototype);
  core.unary = (service: string, method: string, request: any) => {
    unary.push({ service, method, request });
    combined.push(method);
    if (method === "RegisterUpload") {
      return Promise.resolve({ file_id: "f-1", upload_url: "https://example.invalid/put/f-1" });
    }
    return Promise.resolve({ file_id: "f-1" });
  };
  const storage = new StorageFacade(core as UdbCore);
  const puts: { url: string; contentType: string }[] = [];
  await storage.uploadFile("a.png", Buffer.from("hello"), {
    contentType: "image/png",
    putFn: async (url, contentType) => {
      combined.push("PUT");
      puts.push({ url, contentType });
    },
  });

  // The full ordered effect log (incl. the PUT token) equals the shared fixture.
  assert.deepEqual(combined, expectedSeq, `uploadFile sequence drifted from workflow-sequences.md (${expectedSeq.join(", ")})`);
  // The gRPC-only projection of the fixture (drop the PUT token) equals the unary
  // methods emitted — the manifest is the source of truth for the RPC list too.
  assert.deepEqual(
    unary.map((c) => c.method),
    expectedSeq.filter((m) => m !== "PUT"),
  );
  assert.ok(unary.every((c) => c.service === STORAGE));
  // No hidden GetFile / ListFiles / GetDownloadUrl.
  assert.ok(!unary.some((c) => ["GetFile", "ListFiles", "GetDownloadUrl"].includes(c.method)));
  // Exactly one PUT (the single PUT token in the fixture) to the presigned URL.
  assert.equal(expectedSeq.filter((m) => m === "PUT").length, 1);
  assert.deepEqual(puts, [
    { url: "https://example.invalid/put/f-1", contentType: "image/png" },
  ]);
  // Finalize carried the byte length.
  assert.equal(unary.find((c) => c.method === "FinalizeUpload")!.request.size_bytes, 5);
});

test("EntityHandle.upsert = one Upsert; select = one Select (no hidden Get/List)", async () => {
  const { core, unary } = fakeCore();
  const handle = new EntityHandle(core, "acme.v1.Invoice", { key: ["record_id"] }, {
    tenant_id: "acme",
  });

  await handle.upsert({ record_id: "r1", status: "open" });
  await handle.select({ status: "open" });

  // 11.2.3.1: the exact ordered RPC sequence comes from the SHARED
  // workflow-sequences.md fixture (Entity.upsert = one Upsert, Entity.select = one
  // Select), not an inline literal. Concatenated, that is the full effect log.
  const upsertSeq = loadWorkflowSequence("Entity.upsert");
  const selectSeq = loadWorkflowSequence("Entity.select");
  assert.deepEqual(
    unary.map((c) => c.method),
    [...upsertSeq, ...selectSeq],
  );
  assert.ok(unary.every((c) => c.service === DATABROKER));
  const up = unary.find((c) => c.method === "Upsert")!.request;
  assert.deepEqual(up.conflict_fields, ["record_id"]);
  assert.ok(Buffer.isBuffer(up.record_json));
  assert.equal(up.message_type, "acme.v1.Invoice");
  const sel = unary.find((c) => c.method === "Select")!.request;
  assert.deepEqual(sel.filter, { status: "open" });
  // No proof reads.
  assert.ok(!unary.some((c) => ["GetFile", "ListFiles"].includes(c.method)));
});

test("loginAndAdoptTenant = [Login, AuthenticateBearer] and adopts verified tenant", async () => {
  const calls: string[] = [];
  const project: any = Object.create(UdbProject.prototype);
  project.sharedMeta = { tenantId: "acme", projectId: "" };
  // Stub login() to record + return a bearer with NO trustworthy tenant of its own.
  project.login = async () => {
    calls.push("Login");
    return { access_token: "tok-123", mfa_required: false };
  };
  project.setTenant = (t: string) => {
    project.sharedMeta.tenantId = t;
  };
  project.auth = {
    authenticateBearer: async (token: string) => {
      calls.push("AuthenticateBearer");
      assert.equal(token, "tok-123");
      return {
        principal: {
          tenant_id: "11111111-2222-3333-4444-555555555555",
          project_id: "proj-1",
        },
      };
    },
  };

  await UdbProject.prototype.loginAndAdoptTenant.call(project, {
    username: "u",
    password: "p",
  });

  // Exactly Login then AuthenticateBearer — no conditional skip, no extra RPC.
  assert.deepEqual(calls, ["Login", "AuthenticateBearer"]);
  assert.equal(project.sharedMeta.tenantId, "11111111-2222-3333-4444-555555555555");
  assert.equal(project.sharedMeta.projectId, "proj-1");
});

// ── R2.1 naming-contract aliases: exact-RPC mock-sequence gates ───────────────

// A capturing fake UdbAuthClient: records every raw authz RPC the AuthzFacade
// emits. Only the surfaces allowRole/bindRole touch are populated, so any hidden
// List/Get would show up as an extra recorded call.
function fakeAuthClient(): { client: any; calls: { method: string; request: any }[] } {
  const calls: { method: string; request: any }[] = [];
  const client: any = {
    meta: { tenantId: "acme", projectId: "" },
    createPolicyRule: (request: any) => {
      calls.push({ method: "CreatePolicyRule", request });
      return Promise.resolve({ policy: { policy_id: "p-1" } });
    },
    assignRole: (request: any) => {
      calls.push({ method: "AssignRole", request });
      return Promise.resolve({ user_role: { id: "ur-1" } });
    },
    // Surfaces that, if touched, would prove a hidden round trip. AuthzCache reads
    // these off the client; we record them too so a hidden call is caught.
    can: (...a: any[]) => {
      calls.push({ method: "Authorize(can)", request: a });
      return Promise.resolve([true, {}]);
    },
  };
  return { client, calls };
}

test("AuthzFacade.allowRole emits EXACTLY one CreatePolicyRule (no hidden List/Get)", async () => {
  const { client, calls } = fakeAuthClient();
  const authz = new AuthzFacade(client);

  await authz.allowRole("reader", { resource: "invoice", action: "data.select" });

  assert.deepEqual(
    calls.map((c) => c.method),
    ["CreatePolicyRule"],
  );
  assert.deepEqual(calls[0].request, {
    subject: "reader",
    object: "invoice",
    action: "data.select",
    effect: "ALLOW",
  });
});

test("AuthzFacade.bindRole emits EXACTLY one AssignRole (no hidden List/Get)", async () => {
  const { client, calls } = fakeAuthClient();
  const authz = new AuthzFacade(client);

  await authz.bindRole("alice", "reader");

  assert.deepEqual(
    calls.map((c) => c.method),
    ["AssignRole"],
  );
  assert.deepEqual(calls[0].request, {
    user_id: "alice",
    principal_id: "alice",
    role_id: "reader",
  });
});

test("UdbAuthClient.createPolicyRule/assignRole emit exactly their RPC + default tenant", async () => {
  // Drive the raw client over a capturing proto-stub so the tenant/domain
  // defaulting is exercised without a live channel.
  const { UdbAuthClient } = await import("./auth");
  const recorded: { method: string; request: any }[] = [];
  const client: any = Object.create(UdbAuthClient.prototype);
  client.meta = { tenantId: "acme", projectId: "proj-1" };
  client.authz = {
    CreatePolicyRule: (req: any, _md: any, cb: any) => {
      recorded.push({ method: "CreatePolicyRule", request: req });
      cb(null, { policy: {} });
    },
    AssignRole: (req: any, _md: any, cb: any) => {
      recorded.push({ method: "AssignRole", request: req });
      cb(null, { user_role: {} });
    },
  };

  await UdbAuthClient.prototype.createPolicyRule.call(client, {
    subject: "reader",
    object: "invoice",
    action: "data.select",
    effect: "ALLOW",
  });
  await UdbAuthClient.prototype.assignRole.call(client, {
    user_id: "alice",
    principal_id: "alice",
    role_id: "reader",
  });

  assert.deepEqual(
    recorded.map((c) => c.method),
    ["CreatePolicyRule", "AssignRole"],
  );
  // Tenant/project defaulted from bound metadata.
  assert.equal(recorded[0].request.tenant_id, "acme");
  assert.equal(recorded[0].request.project_id, "proj-1");
  assert.equal(recorded[0].request.subject, "reader");
  // AssignRole also defaults domain = tenant.
  assert.equal(recorded[1].request.tenant_id, "acme");
  assert.equal(recorded[1].request.domain, "acme");
  assert.equal(recorded[1].request.role_id, "reader");
});

test("StorageFacade.downloadFile (default) emits EXACTLY one GetDownloadUrl (presigned, no streaming)", async () => {
  const { core, unary, streams } = fakeCore();
  const storage = new StorageFacade(core);

  await storage.downloadFile("f-9", { disposition: "attachment" });

  assert.deepEqual(
    unary.map((c) => [c.service, c.method]),
    [[STORAGE, "GetDownloadUrl"]],
  );
  assert.deepEqual(unary[0].request, { file_id: "f-9", disposition: "attachment" });
  // Presigned path issues NO streaming RPC.
  assert.equal(streams.length, 0);
});

test("StorageFacade.downloadFile({ stream: true }) issues exactly one DownloadFile and reassembles chunks", async () => {
  const { core, unary, streams } = fakeCore();
  // Override serverStream with an async-iterable that yields DownloadFileChunks
  // (the wire shape: { data, contentType?, totalSize?, etag? }). The first chunk
  // carries metadata; bytes split across chunks must reassemble in order.
  (core as any).serverStream = (service: string, method: string, request: any) => {
    streams.push({ service, method, request });
    async function* gen() {
      yield { data: new Uint8Array([1, 2, 3]), contentType: "application/pdf", totalSize: 5n };
      yield { data: new Uint8Array([]) }; // empty frame must be tolerated
      yield { data: new Uint8Array([4, 5]) };
    }
    return gen() as any;
  };
  const storage = new StorageFacade(core);

  const bytes = await storage.downloadFile("f-stream", { stream: true, chunk_size_bytes: 65536 });

  // Exactly one DownloadFile server-stream on StorageService; NO presigned unary.
  assert.deepEqual(
    streams.map((c) => [c.service, c.method]),
    [[STORAGE, "DownloadFile"]],
  );
  assert.equal(unary.length, 0);
  // The `stream` flag is stripped from the request; remaining opts are forwarded.
  assert.deepEqual(streams[0].request, { file_id: "f-stream", chunk_size_bytes: 65536 });
  // Chunks reassemble into the full byte sequence, in order.
  assert.ok(bytes instanceof Uint8Array);
  assert.deepEqual(Array.from(bytes), [1, 2, 3, 4, 5]);
});

test("StorageFacade.downloadFileBytes reassembles DownloadFile chunks", async () => {
  const { core, streams } = fakeCore();
  (core as any).serverStream = (service: string, method: string, request: any) => {
    streams.push({ service, method, request });
    async function* gen() {
      yield { data: new Uint8Array([9]) };
      yield { data: new Uint8Array([8, 7]) };
    }
    return gen() as any;
  };
  const storage = new StorageFacade(core);

  const bytes = await storage.downloadFileBytes("f-7");

  assert.deepEqual(
    streams.map((c) => [c.service, c.method]),
    [[STORAGE, "DownloadFile"]],
  );
  assert.deepEqual(streams[0].request, { file_id: "f-7" });
  assert.deepEqual(Array.from(bytes), [9, 8, 7]);
});

test("EntityHandle.select accepts the contract { where } form (one Select)", async () => {
  const { core, unary } = fakeCore();
  const handle = new EntityHandle(core, "acme.v1.Invoice", { key: ["id"] }, {
    tenant_id: "acme",
  });

  await handle.select({ where: { status: "open" }, limit: 5 });

  assert.deepEqual(
    unary.map((c) => c.method),
    ["Select"],
  );
  assert.deepEqual(unary[0].request.filter, { status: "open" });
  assert.equal(unary[0].request.limit, 5);
});

test("EntityHandle.select still accepts the legacy (where, opts) form", async () => {
  const { core, unary } = fakeCore();
  const handle = new EntityHandle(core, "acme.v1.Invoice", { key: ["id"] }, {});

  await handle.select({ status: "closed" }, { limit: 3 });

  assert.deepEqual(unary[0].request.filter, { status: "closed" });
  assert.equal(unary[0].request.limit, 3);
});

test("UdbProject.connect builds a project; createUdb/Udb.project aliases remain", async () => {
  const { createUdb, Udb } = await import("./project");
  const cfg = { target: "data.invalid:50051", tenantId: "t" };
  const viaConnect = await UdbProject.connect(cfg);
  try {
    assert.ok(viaConnect instanceof UdbProject);
    const viaFactory = createUdb(cfg);
    const viaNamespace = Udb.project(cfg);
    assert.ok(viaFactory instanceof UdbProject);
    assert.ok(viaNamespace instanceof UdbProject);
    // metadata receipt/fence accessor is present per the naming contract.
    assert.equal(typeof viaConnect.metadata.afterWrite, "function");
    viaFactory.close();
    viaNamespace.close();
  } finally {
    viaConnect.close();
  }
});

test("UdbProject wires native facades to authTarget core", () => {
  const project = new UdbProject({
    target: "data.invalid:50051",
    authTarget: "auth.invalid:50052",
    tenantId: "tenant-a",
  });

  try {
    assert.equal((project as any).core.opts.target, "data.invalid:50051");
    assert.equal((project as any).authGenerated.core.opts.target, "auth.invalid:50052");

    for (const facade of [
      project.apikey,
      project.tenant,
      project.notification,
      project.analytics,
      project.asset,
    ] as any[]) {
      assert.equal(facade.core.opts.target, "auth.invalid:50052");
    }

    assert.equal((project.storage as any).core.opts.target, "auth.invalid:50052");
    assert.equal((project.storage as any).objectCore.opts.target, "data.invalid:50051");
  } finally {
    project.close();
  }
});
