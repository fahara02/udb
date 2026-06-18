"use strict";
// Facade unit tests (Phase 7 M8). Pure unit tests with no live server: a
// capturing fake `UdbCore` records the exact (serviceFull, method, request) each
// wrapper emits, so we assert the storage / asset / webrtc facades route to the
// right RPC and build the right request. Run with Node's built-in runner over
// compiled JS:
//   npx tsc -p tsconfig.test.json && node --test dist-test
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const node_fs_1 = require("node:fs");
const path = __importStar(require("node:path"));
const entity_1 = require("./entity");
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
// ── shared workflow-sequence fixture (chapter 11.2.x) ─────────────────────────
// docs/bench-bodies/workflow-sequences.md is the cross-SDK SINGLE SOURCE OF TRUTH
// for the exact ordered RPC sequence each workflow helper may emit (`| helper |
// sequence |` table, col2 = comma-separated ordered method names; the literal
// token `PUT` = the presigned-URL byte transfer, not a gRPC call). The TS / Go /
// Python mock-transport gates each read THIS file instead of carrying an inline
// list, so a drift in the contract fails every language identically.
function workflowSequencesPath() {
    const candidates = [
        path.resolve(__dirname, "../../../docs/bench-bodies/workflow-sequences.md"), // dev: dist-test/
        path.resolve(__dirname, "../../docs/bench-bodies/workflow-sequences.md"),
        path.resolve(__dirname, "../docs/bench-bodies/workflow-sequences.md"),
    ];
    for (const c of candidates) {
        try {
            (0, node_fs_1.readdirSync)(path.dirname(c));
            (0, node_fs_1.readFileSync)(c, "utf8");
            return c;
        }
        catch {
            /* not this candidate */
        }
    }
    return candidates[0];
}
function loadWorkflowSequence(helper) {
    const text = (0, node_fs_1.readFileSync)(workflowSequencesPath(), "utf8");
    for (const line of text.split(/\r?\n/)) {
        const cells = line.split("|").map((c) => c.trim());
        // Data row: `| helper | sequence |` → ["", helper, sequence, ""].
        if (cells.length < 4)
            continue;
        if (cells[1] !== helper)
            continue;
        return cells[2].split(",").map((m) => m.trim()).filter((m) => m.length > 0);
    }
    throw new Error(`workflow-sequences.md has no row for helper "${helper}"`);
}
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
(0, node_test_1.test)("StorageFacade.uploadFile = RegisterUpload + PUT + FinalizeUpload (no hidden reads)", async () => {
    // 11.2.2.2: assert against the SHARED workflow-sequences.md fixture, not an
    // inline literal — the gate fails identically across SDKs if the contract drifts.
    const expectedSeq = loadWorkflowSequence("StorageFacade.uploadFile");
    const unary = [];
    // Combined ordered log of EVERY effect (gRPC method names + the `PUT` token at
    // the moment the presigned byte transfer happens) → compared to the full fixture.
    const combined = [];
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.unary = (service, method, request) => {
        unary.push({ service, method, request });
        combined.push(method);
        if (method === "RegisterUpload") {
            return Promise.resolve({ file_id: "f-1", upload_url: "https://example.invalid/put/f-1" });
        }
        return Promise.resolve({ file_id: "f-1" });
    };
    const storage = new project_1.StorageFacade(core);
    const puts = [];
    await storage.uploadFile("a.png", Buffer.from("hello"), {
        contentType: "image/png",
        putFn: async (url, contentType) => {
            combined.push("PUT");
            puts.push({ url, contentType });
        },
    });
    // The full ordered effect log (incl. the PUT token) equals the shared fixture.
    node_assert_1.strict.deepEqual(combined, expectedSeq, `uploadFile sequence drifted from workflow-sequences.md (${expectedSeq.join(", ")})`);
    // The gRPC-only projection of the fixture (drop the PUT token) equals the unary
    // methods emitted — the manifest is the source of truth for the RPC list too.
    node_assert_1.strict.deepEqual(unary.map((c) => c.method), expectedSeq.filter((m) => m !== "PUT"));
    node_assert_1.strict.ok(unary.every((c) => c.service === STORAGE));
    // No hidden GetFile / ListFiles / GetDownloadUrl.
    node_assert_1.strict.ok(!unary.some((c) => ["GetFile", "ListFiles", "GetDownloadUrl"].includes(c.method)));
    // Exactly one PUT (the single PUT token in the fixture) to the presigned URL.
    node_assert_1.strict.equal(expectedSeq.filter((m) => m === "PUT").length, 1);
    node_assert_1.strict.deepEqual(puts, [
        { url: "https://example.invalid/put/f-1", contentType: "image/png" },
    ]);
    // Finalize carried the byte length.
    node_assert_1.strict.equal(unary.find((c) => c.method === "FinalizeUpload").request.size_bytes, 5);
});
(0, node_test_1.test)("EntityHandle.upsert = one Upsert; select = one Select (no hidden Get/List)", async () => {
    const { core, unary } = fakeCore();
    const handle = new entity_1.EntityHandle(core, "acme.v1.Invoice", { key: ["record_id"] }, {
        tenant_id: "acme",
    });
    await handle.upsert({ record_id: "r1", status: "open" });
    await handle.select({ status: "open" });
    // 11.2.3.1: the exact ordered RPC sequence comes from the SHARED
    // workflow-sequences.md fixture (Entity.upsert = one Upsert, Entity.select = one
    // Select), not an inline literal. Concatenated, that is the full effect log.
    const upsertSeq = loadWorkflowSequence("Entity.upsert");
    const selectSeq = loadWorkflowSequence("Entity.select");
    node_assert_1.strict.deepEqual(unary.map((c) => c.method), [...upsertSeq, ...selectSeq]);
    node_assert_1.strict.ok(unary.every((c) => c.service === DATABROKER));
    const up = unary.find((c) => c.method === "Upsert").request;
    node_assert_1.strict.deepEqual(up.conflict_fields, ["record_id"]);
    node_assert_1.strict.ok(Buffer.isBuffer(up.record_json));
    node_assert_1.strict.equal(up.message_type, "acme.v1.Invoice");
    const sel = unary.find((c) => c.method === "Select").request;
    node_assert_1.strict.deepEqual(sel.filter, { status: "open" });
    // No proof reads.
    node_assert_1.strict.ok(!unary.some((c) => ["GetFile", "ListFiles"].includes(c.method)));
});
(0, node_test_1.test)("loginAndAdoptTenant = [Login, AuthenticateBearer] and adopts verified tenant", async () => {
    const calls = [];
    const project = Object.create(project_1.UdbProject.prototype);
    project.sharedMeta = { tenantId: "acme", projectId: "" };
    // Stub login() to record + return a bearer with NO trustworthy tenant of its own.
    project.login = async () => {
        calls.push("Login");
        return { access_token: "tok-123", mfa_required: false };
    };
    project.setTenant = (t) => {
        project.sharedMeta.tenantId = t;
    };
    project.auth = {
        authenticateBearer: async (token) => {
            calls.push("AuthenticateBearer");
            node_assert_1.strict.equal(token, "tok-123");
            return {
                principal: {
                    tenant_id: "11111111-2222-3333-4444-555555555555",
                    project_id: "proj-1",
                },
            };
        },
    };
    await project_1.UdbProject.prototype.loginAndAdoptTenant.call(project, {
        username: "u",
        password: "p",
    });
    // Exactly Login then AuthenticateBearer — no conditional skip, no extra RPC.
    node_assert_1.strict.deepEqual(calls, ["Login", "AuthenticateBearer"]);
    node_assert_1.strict.equal(project.sharedMeta.tenantId, "11111111-2222-3333-4444-555555555555");
    node_assert_1.strict.equal(project.sharedMeta.projectId, "proj-1");
});
// ── R2.1 naming-contract aliases: exact-RPC mock-sequence gates ───────────────
// A capturing fake UdbAuthClient: records every raw authz RPC the AuthzFacade
// emits. Only the surfaces allowRole/bindRole touch are populated, so any hidden
// List/Get would show up as an extra recorded call.
function fakeAuthClient() {
    const calls = [];
    const client = {
        meta: { tenantId: "acme", projectId: "" },
        createPolicyRule: (request) => {
            calls.push({ method: "CreatePolicyRule", request });
            return Promise.resolve({ policy: { policy_id: "p-1" } });
        },
        assignRole: (request) => {
            calls.push({ method: "AssignRole", request });
            return Promise.resolve({ user_role: { id: "ur-1" } });
        },
        // Surfaces that, if touched, would prove a hidden round trip. AuthzCache reads
        // these off the client; we record them too so a hidden call is caught.
        can: (...a) => {
            calls.push({ method: "Authorize(can)", request: a });
            return Promise.resolve([true, {}]);
        },
    };
    return { client, calls };
}
(0, node_test_1.test)("AuthzFacade.allowRole emits EXACTLY one CreatePolicyRule (no hidden List/Get)", async () => {
    const { client, calls } = fakeAuthClient();
    const authz = new project_1.AuthzFacade(client);
    await authz.allowRole("reader", { resource: "invoice", action: "data.select" });
    node_assert_1.strict.deepEqual(calls.map((c) => c.method), ["CreatePolicyRule"]);
    node_assert_1.strict.deepEqual(calls[0].request, {
        subject: "reader",
        object: "invoice",
        action: "data.select",
        effect: "ALLOW",
    });
});
(0, node_test_1.test)("AuthzFacade.bindRole emits EXACTLY one AssignRole (no hidden List/Get)", async () => {
    const { client, calls } = fakeAuthClient();
    const authz = new project_1.AuthzFacade(client);
    await authz.bindRole("alice", "reader");
    node_assert_1.strict.deepEqual(calls.map((c) => c.method), ["AssignRole"]);
    node_assert_1.strict.deepEqual(calls[0].request, {
        user_id: "alice",
        principal_id: "alice",
        role_id: "reader",
    });
});
(0, node_test_1.test)("UdbAuthClient.createPolicyRule/assignRole emit exactly their RPC + default tenant", async () => {
    // Drive the raw client over a capturing proto-stub so the tenant/domain
    // defaulting is exercised without a live channel.
    const { UdbAuthClient } = await Promise.resolve().then(() => __importStar(require("./auth")));
    const recorded = [];
    const client = Object.create(UdbAuthClient.prototype);
    client.meta = { tenantId: "acme", projectId: "proj-1" };
    client.authz = {
        CreatePolicyRule: (req, _md, cb) => {
            recorded.push({ method: "CreatePolicyRule", request: req });
            cb(null, { policy: {} });
        },
        AssignRole: (req, _md, cb) => {
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
    node_assert_1.strict.deepEqual(recorded.map((c) => c.method), ["CreatePolicyRule", "AssignRole"]);
    // Tenant/project defaulted from bound metadata.
    node_assert_1.strict.equal(recorded[0].request.tenant_id, "acme");
    node_assert_1.strict.equal(recorded[0].request.project_id, "proj-1");
    node_assert_1.strict.equal(recorded[0].request.subject, "reader");
    // AssignRole also defaults domain = tenant.
    node_assert_1.strict.equal(recorded[1].request.tenant_id, "acme");
    node_assert_1.strict.equal(recorded[1].request.domain, "acme");
    node_assert_1.strict.equal(recorded[1].request.role_id, "reader");
});
(0, node_test_1.test)("StorageFacade.downloadFile (default) emits EXACTLY one GetDownloadUrl (presigned, no streaming)", async () => {
    const { core, unary, streams } = fakeCore();
    const storage = new project_1.StorageFacade(core);
    await storage.downloadFile("f-9", { disposition: "attachment" });
    node_assert_1.strict.deepEqual(unary.map((c) => [c.service, c.method]), [[STORAGE, "GetDownloadUrl"]]);
    node_assert_1.strict.deepEqual(unary[0].request, { file_id: "f-9", disposition: "attachment" });
    // Presigned path issues NO streaming RPC.
    node_assert_1.strict.equal(streams.length, 0);
});
(0, node_test_1.test)("StorageFacade.downloadFile({ stream: true }) issues exactly one DownloadFile and reassembles chunks", async () => {
    const { core, unary, streams } = fakeCore();
    // Override serverStream with an async-iterable that yields DownloadFileChunks
    // (the wire shape: { data, contentType?, totalSize?, etag? }). The first chunk
    // carries metadata; bytes split across chunks must reassemble in order.
    core.serverStream = (service, method, request) => {
        streams.push({ service, method, request });
        async function* gen() {
            yield { data: new Uint8Array([1, 2, 3]), contentType: "application/pdf", totalSize: 5n };
            yield { data: new Uint8Array([]) }; // empty frame must be tolerated
            yield { data: new Uint8Array([4, 5]) };
        }
        return gen();
    };
    const storage = new project_1.StorageFacade(core);
    const bytes = await storage.downloadFile("f-stream", { stream: true, chunk_size_bytes: 65536 });
    // Exactly one DownloadFile server-stream on StorageService; NO presigned unary.
    node_assert_1.strict.deepEqual(streams.map((c) => [c.service, c.method]), [[STORAGE, "DownloadFile"]]);
    node_assert_1.strict.equal(unary.length, 0);
    // The `stream` flag is stripped from the request; remaining opts are forwarded.
    node_assert_1.strict.deepEqual(streams[0].request, { file_id: "f-stream", chunk_size_bytes: 65536 });
    // Chunks reassemble into the full byte sequence, in order.
    node_assert_1.strict.ok(bytes instanceof Uint8Array);
    node_assert_1.strict.deepEqual(Array.from(bytes), [1, 2, 3, 4, 5]);
});
(0, node_test_1.test)("StorageFacade.downloadFileBytes reassembles DownloadFile chunks", async () => {
    const { core, streams } = fakeCore();
    core.serverStream = (service, method, request) => {
        streams.push({ service, method, request });
        async function* gen() {
            yield { data: new Uint8Array([9]) };
            yield { data: new Uint8Array([8, 7]) };
        }
        return gen();
    };
    const storage = new project_1.StorageFacade(core);
    const bytes = await storage.downloadFileBytes("f-7");
    node_assert_1.strict.deepEqual(streams.map((c) => [c.service, c.method]), [[STORAGE, "DownloadFile"]]);
    node_assert_1.strict.deepEqual(streams[0].request, { file_id: "f-7" });
    node_assert_1.strict.deepEqual(Array.from(bytes), [9, 8, 7]);
});
(0, node_test_1.test)("EntityHandle.select accepts the contract { where } form (one Select)", async () => {
    const { core, unary } = fakeCore();
    const handle = new entity_1.EntityHandle(core, "acme.v1.Invoice", { key: ["id"] }, {
        tenant_id: "acme",
    });
    await handle.select({ where: { status: "open" }, limit: 5 });
    node_assert_1.strict.deepEqual(unary.map((c) => c.method), ["Select"]);
    node_assert_1.strict.deepEqual(unary[0].request.filter, { status: "open" });
    node_assert_1.strict.equal(unary[0].request.limit, 5);
});
(0, node_test_1.test)("EntityHandle.select still accepts the legacy (where, opts) form", async () => {
    const { core, unary } = fakeCore();
    const handle = new entity_1.EntityHandle(core, "acme.v1.Invoice", { key: ["id"] }, {});
    await handle.select({ status: "closed" }, { limit: 3 });
    node_assert_1.strict.deepEqual(unary[0].request.filter, { status: "closed" });
    node_assert_1.strict.equal(unary[0].request.limit, 3);
});
(0, node_test_1.test)("UdbProject.connect builds a project; createUdb/Udb.project aliases remain", async () => {
    const { createUdb, Udb } = await Promise.resolve().then(() => __importStar(require("./project")));
    const cfg = { target: "data.invalid:50051", tenantId: "t" };
    const viaConnect = await project_1.UdbProject.connect(cfg);
    try {
        node_assert_1.strict.ok(viaConnect instanceof project_1.UdbProject);
        const viaFactory = createUdb(cfg);
        const viaNamespace = Udb.project(cfg);
        node_assert_1.strict.ok(viaFactory instanceof project_1.UdbProject);
        node_assert_1.strict.ok(viaNamespace instanceof project_1.UdbProject);
        // metadata receipt/fence accessor is present per the naming contract.
        node_assert_1.strict.equal(typeof viaConnect.metadata.afterWrite, "function");
        viaFactory.close();
        viaNamespace.close();
    }
    finally {
        viaConnect.close();
    }
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
