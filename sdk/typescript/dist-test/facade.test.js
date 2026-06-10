"use strict";
// Facade unit tests (Phase 7 M8). Pure unit tests with no live server: a
// capturing fake `UdbCore` records the exact (serviceFull, method, request) each
// wrapper emits, so we assert the storage / asset / webrtc facades route to the
// right RPC and build the right request. Run with Node's built-in runner over
// compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const generatedClient_1 = require("./generatedClient");
const project_1 = require("./project");
// A capturing fake UdbCore: skips the real proto loader / gRPC channel and just
// records each invoker call. Only the surfaces the facades touch are populated.
function fakeCore() {
    const unary = [];
    const streams = [];
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.unary = (service, method, request) => {
        unary.push({ service, method, request });
        return Promise.resolve({ ok: true });
    };
    core.serverStream = (service, method, request) => {
        streams.push({ service, method, request });
        return { kind: "serverStream" };
    };
    core.clientStream = (service, method) => {
        streams.push({ service, method, request: undefined });
        return { stream: {}, response: Promise.resolve({}) };
    };
    core.bidiStream = (service, method) => {
        streams.push({ service, method, request: undefined });
        return { kind: "duplex" };
    };
    return { core: core, unary, streams };
}
const STORAGE = "udb.core.storage.services.v1.StorageService";
const DATABROKER = "udb.services.v1.DataBroker";
const ASSET = "udb.core.asset.services.v1.AssetService";
const APIKEY = "udb.core.apikey.services.v1.ApiKeyService";
(0, node_test_1.test)("ApiKeyFacade rotates through atomic RotateApiKey RPC", async () => {
    const { core, unary } = fakeCore();
    const apiKey = new project_1.ApiKeyFacade(core);
    await apiKey.rotate("key-1", { rotation_reason: "scheduled", context: { tenant_id: "acme" } });
    node_assert_1.strict.deepEqual(unary, [
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
(0, node_test_1.test)("StorageFacade routes file-lifecycle RPCs to StorageService", async () => {
    const { core, unary } = fakeCore();
    const storage = new project_1.StorageFacade(core);
    await storage.registerUpload({ tenant_id: "acme", file_name: "a.png" });
    await storage.finalizeUpload({ tenant_id: "acme", file_id: "f1" });
    await storage.getDownloadUrl("f1");
    await storage.getFile("f1");
    await storage.updateFile({ tenant_id: "acme", file_id: "f1" });
    await storage.deleteFile("f1");
    await storage.listFiles();
    const methods = unary.map((c) => c.method);
    node_assert_1.strict.deepEqual(methods, [
        "RegisterUpload",
        "FinalizeUpload",
        "GetDownloadUrl",
        "GetFile",
        "UpdateFile",
        "DeleteFile",
        "ListFiles",
    ]);
    node_assert_1.strict.ok(unary.every((c) => c.service === STORAGE));
    // String-id convenience expands to the right field name.
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetFile").request, { file_id: "f1" });
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "DeleteFile").request, { file_id: "f1" });
});
(0, node_test_1.test)("StorageFacade retains DataBroker object-byte IO escape hatches", async () => {
    const { core, unary, streams } = fakeCore();
    const storage = new project_1.StorageFacade(core);
    storage.putObject();
    storage.getObject({ key: "k" });
    await storage.presign({ key: "k" });
    node_assert_1.strict.deepEqual(streams.map((c) => [c.service, c.method]), [
        [DATABROKER, "PutObject"],
        [DATABROKER, "GetObject"],
    ]);
    // presign goes through the DataBroker object surface, not StorageService.
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GeneratePresignedUrl"), {
        service: DATABROKER,
        method: "GeneratePresignedUrl",
        request: { key: "k" },
    });
});
(0, node_test_1.test)("AssetFacade routes pipeline + asset RPCs to AssetService", async () => {
    const { core, unary } = fakeCore();
    const asset = new project_1.AssetFacade(core);
    await asset.createPipelineDefinition({ tenant_id: "acme" });
    await asset.getPipelineDefinition("d1");
    await asset.registerAsset({ tenant_id: "acme" });
    await asset.startPipeline({ tenant_id: "acme", asset_id: "a1" });
    await asset.getPipeline("i1");
    await asset.completeStep({ tenant_id: "acme", instance_id: "i1" });
    await asset.listAssets();
    await asset.getAsset("a1");
    const methods = unary.map((c) => c.method);
    node_assert_1.strict.deepEqual(methods, [
        "CreatePipelineDefinition",
        "GetPipelineDefinition",
        "RegisterAsset",
        "StartPipeline",
        "GetPipeline",
        "CompleteStep",
        "ListAssets",
        "GetAsset",
    ]);
    node_assert_1.strict.ok(unary.every((c) => c.service === ASSET));
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetPipelineDefinition").request, {
        definition_id: "d1",
    });
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetPipeline").request, { instance_id: "i1" });
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetAsset").request, { asset_id: "a1" });
});
(0, node_test_1.test)("WebRtcFacade routes to per-service room/peer/track/turn + bidi signal", async () => {
    const { core, unary, streams } = fakeCore();
    const webrtc = new project_1.WebRtcFacade(core);
    await webrtc.room.createRoom({ tenant_id: "acme" });
    await webrtc.room.getRoom("r1");
    await webrtc.room.closeRoom("r1");
    await webrtc.peer.joinRoom({ tenant_id: "acme", room_id: "r1" });
    await webrtc.peer.getPeer("p1");
    await webrtc.track.publishTrack({ tenant_id: "acme", peer_id: "p1" });
    await webrtc.track.muteTrack({ tenant_id: "acme", track_id: "t1" });
    await webrtc.turn.issueCredentials({ tenant_id: "acme", room_id: "r1" });
    node_assert_1.strict.deepEqual(unary.map((c) => [c.service, c.method]), [
        ["udb.core.webrtc.services.v1.RoomService", "CreateRoom"],
        ["udb.core.webrtc.services.v1.RoomService", "GetRoom"],
        ["udb.core.webrtc.services.v1.RoomService", "CloseRoom"],
        ["udb.core.webrtc.services.v1.PeerService", "JoinRoom"],
        ["udb.core.webrtc.services.v1.PeerService", "GetPeer"],
        ["udb.core.webrtc.services.v1.TrackService", "PublishTrack"],
        ["udb.core.webrtc.services.v1.TrackService", "MuteTrack"],
        ["udb.core.webrtc.services.v1.TurnService", "IssueCredentials"],
    ]);
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetRoom").request, { room_id: "r1" });
    node_assert_1.strict.deepEqual(unary.find((c) => c.method === "GetPeer").request, { peer_id: "p1" });
    // The bidi signalling helper opens SignalingService.Signal.
    webrtc.signal();
    node_assert_1.strict.deepEqual(streams.map((c) => [c.service, c.method]), [
        ["udb.core.webrtc.services.v1.SignalingService", "Signal"],
    ]);
});
(0, node_test_1.test)("UdbProject exposes storage/asset/webrtc facades wired to the shared core", () => {
    // Build a project without the real constructor so no channel/proto load runs;
    // then construct the facades over a fake core exactly as the constructor does.
    const { core } = fakeCore();
    const project = Object.create(project_1.UdbProject.prototype);
    project.storage = new project_1.StorageFacade(core);
    project.asset = new project_1.AssetFacade(core);
    project.webrtc = new project_1.WebRtcFacade(core);
    node_assert_1.strict.ok(project.storage instanceof project_1.StorageFacade);
    node_assert_1.strict.ok(project.asset instanceof project_1.AssetFacade);
    node_assert_1.strict.ok(project.webrtc instanceof project_1.WebRtcFacade);
    // WebRTC sub-clients are namespaced.
    node_assert_1.strict.equal(typeof project.webrtc.room.createRoom, "function");
    node_assert_1.strict.equal(typeof project.webrtc.peer.joinRoom, "function");
    node_assert_1.strict.equal(typeof project.webrtc.track.publishTrack, "function");
    node_assert_1.strict.equal(typeof project.webrtc.turn.issueCredentials, "function");
    node_assert_1.strict.equal(typeof project.webrtc.signal, "function");
});
(0, node_test_1.test)("UdbProject wires native facades to authTarget core", () => {
    const project = new project_1.UdbProject({
        target: "data.invalid:50051",
        authTarget: "auth.invalid:50052",
        tenantId: "tenant-a",
    });
    try {
        node_assert_1.strict.equal(project.core.opts.target, "data.invalid:50051");
        node_assert_1.strict.equal(project.authGenerated.core.opts.target, "auth.invalid:50052");
        for (const facade of [
            project.apikey,
            project.tenant,
            project.notification,
            project.analytics,
            project.asset,
        ]) {
            node_assert_1.strict.equal(facade.core.opts.target, "auth.invalid:50052");
        }
        node_assert_1.strict.equal(project.storage.core.opts.target, "auth.invalid:50052");
        node_assert_1.strict.equal(project.storage.objectCore.opts.target, "data.invalid:50051");
    }
    finally {
        project.close();
    }
});
