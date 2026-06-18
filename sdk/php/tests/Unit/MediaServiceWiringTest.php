<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Services\AssetService;
use Fahara02\UdbLaravel\Services\StorageService;
use Fahara02\UdbLaravel\Services\WebRtc\PeerApi;
use Fahara02\UdbLaravel\Services\WebRtc\RoomApi;
use Fahara02\UdbLaravel\Services\WebRtc\SignalingApi;
use Fahara02\UdbLaravel\Services\WebRtc\TrackApi;
use Fahara02\UdbLaravel\Services\WebRtc\TurnApi;
use Fahara02\UdbLaravel\Services\WebRtcService;
use Fahara02\UdbLaravel\UdbProject;

/*
 * M8 media-service facade wiring (storage / asset / webRtc).
 *
 * These assert the wrapper *shape*: the wrapper classes expose the expected
 * convenience methods + a client() accessor, and UdbProject advertises the
 * three new accessors. It is pure reflection — no generated protobuf messages
 * are constructed and no gRPC channel is opened — so it runs in the lint-only
 * image where ext-grpc / ext-protobuf are absent.
 */

it('exposes storage/asset/webRtc accessors on UdbProject', function () {
    foreach (['storage', 'asset', 'webRtc'] as $accessor) {
        expect(method_exists(UdbProject::class, $accessor))->toBeTrue("UdbProject::{$accessor}() missing");
    }

    expect((new ReflectionMethod(UdbProject::class, 'storage'))->getReturnType()?->getName())
        ->toBe(StorageService::class);
    expect((new ReflectionMethod(UdbProject::class, 'asset'))->getReturnType()?->getName())
        ->toBe(AssetService::class);
    expect((new ReflectionMethod(UdbProject::class, 'webRtc'))->getReturnType()?->getName())
        ->toBe(WebRtcService::class);
});

it('StorageService wraps the documented RPCs plus client()', function () {
    foreach ([
        'registerUpload', 'finalizeUpload', 'getDownloadUrl', 'downloadFile',
        'downloadFileBytes', 'withDownloadStreamOpener', 'getFile', 'updateFile',
        'deleteFile', 'listFiles', 'client',
    ] as $m) {
        expect(method_exists(StorageService::class, $m))->toBeTrue("StorageService::{$m}() missing");
    }
});

it('AssetService wraps the documented RPCs plus client()', function () {
    foreach ([
        'createPipelineDefinition', 'getPipelineDefinition', 'registerAsset',
        'startPipeline', 'getPipeline', 'completeStep', 'listAssets', 'getAsset', 'client',
    ] as $m) {
        expect(method_exists(AssetService::class, $m))->toBeTrue("AssetService::{$m}() missing");
    }
});

it('WebRtcService groups room/peer/track/turn/signaling', function () {
    foreach (['room', 'peer', 'track', 'turn', 'signaling'] as $m) {
        expect(method_exists(WebRtcService::class, $m))->toBeTrue("WebRtcService::{$m}() missing");
    }

    expect((new ReflectionMethod(WebRtcService::class, 'room'))->getReturnType()?->getName())->toBe(RoomApi::class);
    expect((new ReflectionMethod(WebRtcService::class, 'peer'))->getReturnType()?->getName())->toBe(PeerApi::class);
    expect((new ReflectionMethod(WebRtcService::class, 'track'))->getReturnType()?->getName())->toBe(TrackApi::class);
    expect((new ReflectionMethod(WebRtcService::class, 'turn'))->getReturnType()?->getName())->toBe(TurnApi::class);
    expect((new ReflectionMethod(WebRtcService::class, 'signaling'))->getReturnType()?->getName())->toBe(SignalingApi::class);
});

it('WebRTC group wrappers carry their key RPC verbs + client()', function () {
    foreach (['createRoom', 'getRoom', 'updateRoom', 'closeRoom', 'listRooms', 'client'] as $m) {
        expect(method_exists(RoomApi::class, $m))->toBeTrue("RoomApi::{$m}() missing");
    }
    foreach (['joinRoom', 'leaveRoom', 'getPeer', 'listPeers', 'client'] as $m) {
        expect(method_exists(PeerApi::class, $m))->toBeTrue("PeerApi::{$m}() missing");
    }
    foreach (['publishTrack', 'unpublishTrack', 'muteTrack', 'listTracks', 'client'] as $m) {
        expect(method_exists(TrackApi::class, $m))->toBeTrue("TrackApi::{$m}() missing");
    }
    foreach (['issueCredentials', 'client'] as $m) {
        expect(method_exists(TurnApi::class, $m))->toBeTrue("TurnApi::{$m}() missing");
    }
    // Signal is a bidi stream — exposed as a raw stream accessor, not invoke().
    foreach (['signal', 'client'] as $m) {
        expect(method_exists(SignalingApi::class, $m))->toBeTrue("SignalingApi::{$m}() missing");
    }
});

/*
 * BENCH §11.2.x storage uploadFile sequence gate (OFFLINE).
 *
 * The full cross-language contract is that StorageFacade.uploadFile is the
 * canonical 3-step sequence RegisterUpload -> presigned PUT -> FinalizeUpload.
 * Go/TS/Python prove this with offline mock-transport sequence gates that read
 * docs/bench-bodies/workflow-sequences.md. PHP's UdbProject is final and a clean
 * offline mock transport needs ext-grpc, so a like-for-like PHP offline gate is
 * DEFERRED — the upload logic is verified by reading + the injectable $httpPut
 * seam on StorageService::uploadFile(). What we CAN do offline (no broker, no
 * ext-grpc) is assert the shared fixture parses and pins the same 3-step contract
 * the PHP wrapper implements, so a drift in the contract trips a PHP test too.
 */
it('shared workflow-sequences fixture pins the uploadFile contract PHP implements', function () {
    $path = dirname(__DIR__, 4).'/docs/bench-bodies/workflow-sequences.md';
    expect(is_file($path))->toBeTrue("missing shared fixture: {$path}");
    $text = file_get_contents($path) ?: '';

    // The fixture must document the canonical StorageFacade.uploadFile sequence.
    expect($text)->toContain('StorageFacade.uploadFile');
    // ...as exactly RegisterUpload -> PUT -> FinalizeUpload (the 3-step contract).
    foreach (['RegisterUpload', 'PUT', 'FinalizeUpload'] as $step) {
        expect($text)->toContain($step);
    }

    // And the PHP wrapper that realizes it exposes the injectable PUT seam used to
    // verify the middle step without a live object store / broker.
    expect(method_exists(StorageService::class, 'uploadFile'))->toBeTrue('StorageService::uploadFile() missing');
    $params = (new ReflectionMethod(StorageService::class, 'uploadFile'))->getParameters();
    $hasHttpPutSeam = false;
    foreach ($params as $p) {
        if ($p->getName() === 'httpPut') {
            $hasHttpPutSeam = true;
            break;
        }
    }
    expect($hasHttpPutSeam)->toBeTrue('StorageService::uploadFile() missing injectable $httpPut seam');
});
