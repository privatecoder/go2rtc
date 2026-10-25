# PTZ for `bc://` cameras through go2rtc's ONVIF server — design

Date: 2026-10-24. Branch: `feature/bc-ptz` in privatecoder/go2rtc.

## Intent

The maintainer runs Frigate (0.18.x) and Home Assistant on go2rtc, with Reolink cameras pulled over the native Baichuan
source (`bc://`, `pkg/bc`, `internal/bc`). Frigate's PTZ controls only speak ONVIF. Reolink ships ONVIF/RTSP disabled
by default and the maintainer wants them off; two cameras are only reachable by UID over P2P, where the camera's own
ONVIF port is unreachable anyway. The Reolink app moves cameras over Baichuan on the same session it streams on.

So: go2rtc's existing ONVIF server (`internal/onvif`, `pkg/onvif`) should present PTZ for `bc://` streams and translate
the ONVIF PTZ calls into Baichuan PTZ messages sent to the camera. Frigate then points its `onvif:` block at go2rtc.

Success: on the real PTZ+zoom camera (Reolink **E1 Zoom**, single lens, channel 0; local at 192.168.1.50, credentials
supplied by the maintainer at test time, never written to the repo), Frigate's live view shows PTZ arrows, zoom and the preset dropdown, and all of them move the
camera. The same works for a UID (P2P) camera without any extra configuration — verified live with the E1 Zoom addressed by
its own UID (read from the camera after login, never committed).

Live testing **may write presets** on the E1 Zoom (maintainer's answer): `SetPreset` is tested live, using a free or
throw-away slot where possible; existing presets the test overwrites are recorded first and restored afterwards.

## Goals

1. **Manual control** — ONVIF `ContinuousMove` (pan/tilt and zoom) and `Stop`.
2. **Presets** — ONVIF `GetPresets`, `GotoPreset`, and `SetPreset` (Frigate has no save button; HA and other ONVIF
   clients do). `RemovePreset` only if Baichuan clearly supports it; otherwise return an ONVIF fault.
3. Works identically for TCP (IP) and UDP/P2P (UID) `bc://` sources.
4. Streams that are not `bc://`, or `bc://` cameras without PTZ, have exactly today's per-stream ONVIF responses (profiles,
   sources, URIs). The device-wide PTZ XAddr is the one exception, see below.

## Non-goals

- Autotracking. Do **not** advertise `RelativeMove`/FOV translation spaces or `AbsoluteMove`; Frigate must report
  autotracking as unsupported rather than misbehave. (`GetStatus` may be answered if cheap, but it is not required.)
- Focus, patrols, guard position, PTZ calibration, imaging service.
- Native PTZ in go2rtc's own web UI or a new REST API.
- ONVIF authentication (go2rtc's ONVIF server accepts unauthenticated requests today; keep that behaviour — Frigate's
  user/password are sent and ignored).
- Upstream compatibility: this lives only in the fork.

## Anti-goals (a failure even if it "works")

- A camera that keeps moving because a `Stop` was lost. A continuous move must be stopped by `Stop`, and also by a
  server-side safety timeout if no `Stop` arrives (Frigate sends Stop on button release; network or client failures
  must not leave the camera spinning).
- PTZ causing the video stream to reconnect, stall, or open a second Baichuan login while a stream session is already up.
- Breaking existing ONVIF consumers (Home Assistant's ONVIF integration against go2rtc) for non-PTZ streams.
- Credentials, UIDs, IPs or other personal data in commits, tests, fixtures or logs above debug.

## Constraints and decided parts of the how

- **Frigate selects the camera by profile.** Frigate 0.18 config per camera:
  ```yaml
  onvif:
    host: 127.0.0.1      # go2rtc (inside the Frigate container)
    port: 1984
    profile: <go2rtc stream name>
  ```
  Frigate (`frigate/ptz/onvif.py`, v0.18.2) calls: `update_xaddrs` (GetCapabilities/GetServices), media `GetProfiles`,
  keeps only profiles whose `PTZConfiguration` has `DefaultContinuousPanTiltVelocitySpace` or
  `DefaultContinuousZoomVelocitySpace`, picks the one whose token or Name equals `onvif.profile`, then creates the PTZ
  service from the advertised PTZ XAddr, calls `GetConfigurationOptions` (optional), `GetPresets`, and at runtime
  `ContinuousMove`, `Stop` (with `PanTilt`/`Zoom` true), `GotoPreset`. Features shown: `pt` from the pan/tilt velocity
  space, `zoom` from the zoom velocity space. go2rtc's profile token is already the stream name.
- **ONVIF side** (`pkg/onvif`, `internal/onvif`): add a PTZ service XAddr (`/onvif/ptz_service`) to GetCapabilities and
  GetServices *only when at least one `bc://` stream is configured* (orchestrator's call: these responses are device-wide;
  installs without `bc://` streams stay byte-identical, and Frigate's startup init does not have to wait for a camera
  login to see the service); add `PTZConfiguration` to the profile of PTZ-capable `bc://` streams only (pan/tilt velocity space; zoom
  velocity space only if the camera has zoom); handle PTZ operations `GetConfigurations`, `GetConfiguration`,
  `GetConfigurationOptions`, `GetNodes`, `GetNode`, `GetServiceCapabilities`, `GetPresets`, `GotoPreset`, `SetPreset`,
  `ContinuousMove`, `Stop`. Unknown/unsupported PTZ operations return a proper SOAP fault, not HTTP 400 with plain text,
  so ONVIF libraries fail cleanly. Follow the existing style of `pkg/onvif/server.go` (string-built envelopes).
- **Baichuan side** (`pkg/bc`), from neolink (`crates/core/src/bc_protocol/ptz.rs`, `bc/model.rs`, `bc/xml.rs`) and
  `../neolink/docs` (authoritative when sources disagree):
  - move/stop: msg 18 `PtzControl` `{channelId, speed, command}` with `left|right|up|down|leftUp|leftDown|rightUp|rightDown|stop`.
  - presets: msg 190 get list; msg 19 `PtzPreset` with `Preset{id,name,command}` `toPos` / `setPos`.
  - zoom: msg 294/295 zoom/focus (`StartZoomFocus` `zoomPos` absolute position, min/max from 294).
  - PTZ / zoom capability: from the camera's ability/support info (neolink `abilityinfo.rs`, `support.rs`).
- **Session reuse** *(orchestrator's call)*. PTZ is an add-on that never touches the streaming dial/reconnect path.
  PTZ commands go over an already-running Baichuan client for the **same camera** — the profile's own stream, or any other
  active `bc://` producer with the same address/UID and channel (e.g. the `-sub` stream). Only when none is active does
  the PTZ path open a control-only session (same URL/discovery), keep it for a short idle period, then close it. If a video
  stream starts while that control session lingers, a brief overlap of two logins is acceptable (each go2rtc stream already
  logs in on its own today); the control session is simply not reused by the stream and expires. Control sessions to UID
  cameras must respect the existing P2P discovery cooldown and must not retry in a tight loop.
- **Mapping a stream to a camera.** A go2rtc stream may have several sources (`bc://…` plus `ffmpeg:…#audio=opus`); PTZ
  uses the first `bc://` source of the stream named by `ProfileToken`.
- **Speed mapping.** ONVIF velocities are -1..1 per axis. Direction is the sign combination (8 directions); Baichuan
  speed derives from the magnitude, mapped into the camera's range (neolink's default is 32; Reolink HTTP accepts 1–64).
  Zero vector = stop.
- **Testing.** Unit tests against the existing simulated camera (`pkg/bc/fakecam_test.go`) for every Baichuan message,
  and ONVIF request/response tests in `pkg/onvif`. Then live verification on 192.168.1.50 by the orchestrator, plus a
  Frigate-like client check (the `onvif-zeep-async` library Frigate uses, or equivalent SOAP calls) before handing over.
- Follow the repo's commit conventions; the orchestrator drives git. Docs: extend `internal/bc/README.md` (Frigate
  `onvif:` example) and `internal/onvif/README.md`.

## Left to the builder

- Exact ONVIF XML details (node/space URIs, limits), as long as Frigate 0.18.2 and HA's ONVIF integration accept them.
- **Continuous zoom.** Whether Baichuan `PtzControl` accepts a continuous zoom command (HTTP API has `ZoomInc`/`ZoomDec`;
  check neolink docs, `../reolink-app`, `../reolink-native-linux`). If not, emulate continuous zoom by stepping the
  absolute zoom position (msg 295) until `Stop`. Either is acceptable; say which was chosen and why.
- The safety-timeout value for an unstopped continuous move (ONVIF `ContinuousMove` also carries an optional `Timeout`;
  honour it when present).
- How PTZ capability is cached and refreshed, and what `GetProfiles` does when the camera's capability is not yet known
  (e.g. trigger a bounded control-session login vs. report no PTZ until the stream has logged in — prefer the option that
  makes Frigate's one-shot init at startup succeed).
- Package boundaries between `internal/onvif`, `internal/bc` and `pkg/bc`.
