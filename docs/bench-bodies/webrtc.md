## WebRTC (Room/Peer/Track/Turn/Signaling)

_proto: core/webrtc/services/v1/webrtc_service.proto · 16 RPCs_

Seed legend (substitute real values for `<seed:KEY>`): `tenant_id`, `project`, `room_id`, `peer_id`, `track_id`, `unpublish_track_id`, `leave_peer_id`, `signal_peer_id`, `close_room_id`, `user_id`.

All RPCs require `endpoint_security` bearer auth with `tenant_required: true`; `tenant_id` is a real request field on every request message (verified in proto) and must match the authenticated tenant. `config`/`metadata`/`settings` are free-form JSON strings.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | RoomService.CreateRoom | MUTATION | CreateRoomRequest | `tenant_id`=`<seed:tenant_id>`, `name`="bench-room", `max_participants`=10, `config`=`{}`, `created_by`=`<seed:user_id>` | name free text; max_participants int32; config JSON string; created_by user ref. Returns new `room_id`. |
| [ ] | RoomService.GetRoom | READ_ONLY | GetRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>` | room must exist. |
| [ ] | RoomService.UpdateRoom | MUTATION | UpdateRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `name`="bench-room-2", `state`="active", `config`=`{}` | all fields string; `state` is a free-form string column (not a proto enum); config JSON. |
| [ ] | RoomService.CloseRoom | MUTATION | CloseRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:close_room_id>` | room must exist; cascades peer.left/track.ended emits. Uses a disposable room so the main active room remains usable. |
| [ ] | RoomService.ListRooms | READ_ONLY | ListRoomsRequest | `tenant_id`=`<seed:tenant_id>`, `state`="active", `page`=1, `page_size`=20 | `state` filter is optional free-form string; page/page_size int32. |
| [ ] | PeerService.JoinRoom | MUTATION | JoinRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `display_name`="Bench User", `metadata`=`{}`, `user_agent`="bench/1.0" | room must exist+open. metadata JSON string. Returns new `peer_id` + existing_peers. |
| [ ] | PeerService.LeaveRoom | MUTATION | LeaveRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:leave_peer_id>` | peer must be in room. Uses a disposable peer so the main active peer remains usable. |
| [ ] | PeerService.GetPeer | READ_ONLY | GetPeerRequest | `tenant_id`=`<seed:tenant_id>`, `peer_id`=`<seed:peer_id>` | no room_id; peer looked up by id within tenant. |
| [ ] | PeerService.ListPeers | READ_ONLY | ListPeersRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `state`="connected" | `state` optional free-form string filter. |
| [ ] | TrackService.PublishTrack | MUTATION | PublishTrackRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `kind`="audio", `label`="mic", `settings`=`{}`, `metadata`=`{}` | `kind` free-form string (e.g. "audio"/"video"); settings+metadata JSON. peer must exist. Returns new `track_id`. |
| [ ] | TrackService.UnpublishTrack | MUTATION | UnpublishTrackRequest | `tenant_id`=`<seed:tenant_id>`, `track_id`=`<seed:unpublish_track_id>` | track must exist (no room_id needed). Uses a disposable track so `MuteTrack` keeps a live `track_id`. |
| [ ] | TrackService.MuteTrack | MUTATION | MuteTrackRequest | `tenant_id`=`<seed:tenant_id>`, `track_id`=`<seed:track_id>`, `muted`=true | `muted` bool toggles mute state. |
| [ ] | TrackService.ListTracks | READ_ONLY | ListTracksRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `kind`="audio" | peer_id + kind are optional free-form filters. |
| [ ] | TurnService.IssueCredentials | MUTATION | IssueCredentialsRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `ttl_seconds`=3600 | ttl_seconds int32. Returns ephemeral ICE servers + signed username/credential. TURN config must be present (fail-closed). |
| [ ] | SignalingService.Signal | MUTATION | SignalRequest (stream) | per-message: `room_id`=`<seed:room_id>`, `peer_id`=`<seed:signal_peer_id>`, `tenant_id`=`<seed:tenant_id>` + ONE oneof payload: `ping`=true \| `offer_sdp`="<sdp>" \| `answer_sdp`="<sdp>" \| `ice_candidate`="<candidate>" | BIDI stream — uses a disposable joined peer because closing the stream disconnects that peer. Simplest valid frame: set `ping`=true. Server replies SignalResponse oneof (offer_sdp/answer_sdp/ice_candidate/peer_joined/peer_left/track_published/pong). |
