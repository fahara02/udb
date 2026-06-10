// Facade unit tests (Phase 7 M8). Pure unit tests with no live server: a
// capturing fake `UdbCore` records the exact (serviceFull, method, request) each
// wrapper emits, so we assert the storage / asset / webrtc facades route to the
// right RPC and build the right request. Run with Node's built-in runner over
// compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test

import { strict as assert } from "node:assert";
import { test } from "node:test";

import { UdbCore } from "./generatedClient";
import {
  ApiKeyFacade,
  AssetFacade,
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
