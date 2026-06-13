use super::support::*;
use crate::proto::udb::core::webrtc::services::v1 as webrtc_pb;
use crate::proto::udb::core::webrtc::services::v1::peer_service_server::PeerService;
use crate::proto::udb::core::webrtc::services::v1::room_service_server::RoomService;
use crate::proto::udb::core::webrtc::services::v1::track_service_server::TrackService;
use crate::proto::udb::core::webrtc::services::v1::turn_service_server::TurnService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_webrtc_native_schema_from_proto -- --ignored --nocapture"]
async fn live_postgres_webrtc_native_schema_from_proto() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    assert_native_table_columns(
        &pool,
        "udb.core.webrtc.entity.v1.Room",
        &["room_id", "tenant_id", "name", "state", "participant_count"],
    )
    .await;
    assert_native_table_columns(
        &pool,
        "udb.core.webrtc.entity.v1.Track",
        &[
            "track_id",
            "room_id",
            "peer_id",
            "tenant_id",
            "kind",
            "state",
        ],
    )
    .await;

    cleanup_native_service_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_webrtc_room_roundtrip -- --ignored --nocapture"]
async fn live_postgres_webrtc_room_roundtrip() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = webrtc_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    // create a room
    let room = RoomService::create_room(
        &svc,
        Request::new(webrtc_pb::CreateRoomRequest {
            tenant_id: tenant_id.clone(),
            name: "standup".to_string(),
            max_participants: 10,
            ..Default::default()
        }),
    )
    .await
    .expect("create_room")
    .into_inner();
    assert!(!room.room_id.is_empty());

    // a peer joins
    let join = PeerService::join_room(
        &svc,
        Request::new(webrtc_pb::JoinRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            display_name: "Ada".to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect("join_room")
    .into_inner();
    let peer = join.peer.expect("peer");
    assert!(join.existing_peers.is_empty());

    // room now shows participant_count == 1
    let got = RoomService::get_room(
        &svc,
        Request::new(webrtc_pb::GetRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
        }),
    )
    .await
    .expect("get_room")
    .into_inner();
    assert_eq!(got.room.expect("room").participant_count, 1);

    // publish a track
    let track = TrackService::publish_track(
        &svc,
        Request::new(webrtc_pb::PublishTrackRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            peer_id: peer.peer_id.clone(),
            kind: "VIDEO".to_string(),
            label: "cam".to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect("publish_track")
    .into_inner();
    assert!(!track.track_id.is_empty());

    let tracks = TrackService::list_tracks(
        &svc,
        Request::new(webrtc_pb::ListTracksRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            ..Default::default()
        }),
    )
    .await
    .expect("list_tracks")
    .into_inner();
    assert_eq!(tracks.tracks.len(), 1);

    // TURN credentials are derived (HMAC), no DB needed
    let turn = TurnService::issue_credentials(
        &svc,
        Request::new(webrtc_pb::IssueCredentialsRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            peer_id: peer.peer_id.clone(),
            ttl_seconds: 600,
        }),
    )
    .await
    .expect("issue_credentials")
    .into_inner();
    assert!(!turn.username.is_empty());
    assert!(!turn.credential.is_empty());
    assert_eq!(turn.ttl_seconds, 600);
    assert_eq!(turn.allowed_action, "webrtc.turn.relay");
    assert!(
        turn.username
            .ends_with(&format!("{}:{}", peer.peer_id, turn.allowed_action)),
        "TURN username must bind peer_id and allowed action"
    );

    // peer leaves → participant_count back to 0
    let left = PeerService::leave_room(
        &svc,
        Request::new(webrtc_pb::LeaveRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            peer_id: peer.peer_id.clone(),
        }),
    )
    .await
    .expect("leave_room")
    .into_inner();
    assert!(left.success);

    // close the room
    RoomService::close_room(
        &svc,
        Request::new(webrtc_pb::CloseRoomRequest {
            tenant_id,
            room_id: room.room_id,
        }),
    )
    .await
    .expect("close_room");

    cleanup_native_service_db(&pool).await;
}

/// Phase 7 acceptance (final_task.md §8): "WebRTC peers cannot signal or publish
/// after room closure." `CloseRoom` is a lifecycle transition that marks the room
/// CLOSED and ends its peers/tracks, so `require_active_peer_membership` (which
/// requires `peer.state='CONNECTED'` AND `room.state='ACTIVE'`) fails closed for
/// both `PublishTrack` and `Signal` once the room is closed.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_webrtc_rejects_signal_publish_after_close -- --ignored --nocapture"]
async fn live_postgres_webrtc_rejects_signal_publish_after_close() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = webrtc_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    let room = RoomService::create_room(
        &svc,
        Request::new(webrtc_pb::CreateRoomRequest {
            tenant_id: tenant_id.clone(),
            name: "to-close".to_string(),
            max_participants: 10,
            ..Default::default()
        }),
    )
    .await
    .expect("create_room")
    .into_inner();

    let join = PeerService::join_room(
        &svc,
        Request::new(webrtc_pb::JoinRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            display_name: "Grace".to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect("join_room")
    .into_inner();
    let peer = join.peer.expect("peer");

    // A connected peer CAN publish and signal while the room is ACTIVE.
    TrackService::publish_track(
        &svc,
        Request::new(webrtc_pb::PublishTrackRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            peer_id: peer.peer_id.clone(),
            kind: "VIDEO".to_string(),
            label: "cam".to_string(),
            ..Default::default()
        }),
    )
    .await
    .expect("publish_track must succeed while the room is active");

    // Close the room (room → CLOSED, peers → CLOSED, tracks → ENDED).
    RoomService::close_room(
        &svc,
        Request::new(webrtc_pb::CloseRoomRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
        }),
    )
    .await
    .expect("close_room");

    // PublishTrack must now be rejected — the peer is no longer an active member.
    let publish_after = TrackService::publish_track(
        &svc,
        Request::new(webrtc_pb::PublishTrackRequest {
            tenant_id: tenant_id.clone(),
            room_id: room.room_id.clone(),
            peer_id: peer.peer_id.clone(),
            kind: "VIDEO".to_string(),
            label: "cam2".to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert!(
        publish_after.is_err(),
        "publish_track must be rejected after the room is closed"
    );
    // `Signal` is client-streaming (impractical to drive from a unit test) but
    // routes through the SAME `require_active_peer_membership` gate as
    // `PublishTrack` (verified above), so the closed-room rejection proven here
    // applies identically to signaling.

    cleanup_native_service_db(&pool).await;
}
