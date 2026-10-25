# ONVIF PTZ for `bc://` streams — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Frigate (and HA) can pan/tilt, zoom and use presets on Reolink `bc://` cameras by pointing their ONVIF client at go2rtc, which turns ONVIF PTZ calls into Baichuan PTZ messages.

**Architecture:** `pkg/bc` gets PTZ requests on `Client` (msgs 18, 19, 190, 199, 294, 295), a per-camera `PTZ` controller (session reuse/control session, continuous-move safety timer, stepped zoom, cached capabilities) and a registry of active producer clients. `pkg/onvif` gets PTZ SOAP builders/parsers and a SOAP fault. `internal/onvif` routes `/onvif/ptz_service` and maps `ProfileToken` (= stream name) to the stream's first `bc://` source.

**Tech Stack:** Go 1.25, stdlib `encoding/xml`, existing go2rtc `pkg/onvif` string-built envelopes, existing `pkg/bc` fake camera for tests.

**Spec:** `docs/superpowers/specs/2026-10-24-bc-onvif-ptz-design.md` (read it first; `../neolink/docs` wins over other sources).

## Global Constraints

- Only this fork; no upstream compatibility concerns. Ported neolink logic is fine (AGPL note already in `internal/bc/README.md`).
- Never commit credentials, UIDs (use `95270000ABCDEFGH` in tests/docs), IPs other than RFC1918 examples like `192.168.1.50`, or log them above debug.
- The streaming path (`NewProducer` login → `StartStream` → probe, reconnect behaviour) must keep its behaviour; refactors there are extraction only, covered by the existing tests.
- No autotracking: never advertise `RelativeMove`/`AbsoluteMove` spaces or FOV translation spaces.
- Installs without any `bc://` stream must get byte-identical ONVIF responses to today.
- Run before every commit: `go vet ./pkg/bc/... ./pkg/onvif/... ./internal/bc/... ./internal/onvif/...` and `go test -race ./pkg/bc/... ./pkg/onvif/...` — all pass.
- The implementer does not run git; the orchestrator commits each task (message style: `bc: …`, `onvif: …`, trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`).

## Review Focus

1. **Lost Stop** (client crashes, network drop, Frigate restart mid-press) → the camera stops by itself after the safety timeout (default 10 s, or the request's ONVIF `Timeout` if shorter). Test in Task 2.
2. **Camera capability not known yet when Frigate calls GetProfiles at startup** (stream not logged in, UID discovery slow) → GetProfiles answers within 8 s, PTZ appears for every camera whose caps arrived, and caps are prefetched at go2rtc start. Test in Task 4.
3. **Several go2rtc streams for one camera (`cam` main + `cam-sub`)** → PTZ on either profile uses whichever producer client is active; with none active, exactly one control session is opened even under concurrent requests. Test in Task 2.
4. **Out-of-range / odd ONVIF values** (velocity > 1, NaN, only zoom given, `0,0` pan-tilt with zoom, unknown preset token, empty body) → clamped / SOAP fault, never a panic or a moving camera. Tests in Tasks 2 and 3.
5. **Stream with a bc source that is not first, or with a non-bc first source** (`ffmpeg:` first, `bc://` second) → PTZ still found; stream with no bc source → no PTZConfiguration and PTZ ops return a fault. Test in Task 4.

---

### Task 1: Baichuan PTZ requests on `Client`

**Files:**
- Create: `pkg/bc/ptz_msg.go`
- Modify: `pkg/bc/message.go` (msg ID consts), `pkg/bc/xml.go` (Body fields + structs), `pkg/bc/fakecam_test.go` (PTZ handlers + recorded events)
- Test: `pkg/bc/ptz_msg_test.go`

**Interfaces:**
- Produces (all in package `bc`):
  - consts `MsgPTZControl = 18`, `MsgPTZPreset = 19`, `MsgGetPTZPreset = 190`, `MsgGetSupport = 199`, `MsgGetZoomFocus = 294`, `MsgSetZoomFocus = 295`
  - `type Direction string` with `DirLeft="left"`, `DirRight="right"`, `DirUp="up"`, `DirDown="down"`, `DirLeftUp="leftUp"`, `DirLeftDown="leftDown"`, `DirRightUp="rightUp"`, `DirRightDown="rightDown"`, `DirStop="stop"`
  - XML: `PtzControl{Version attr, ChannelID uint8 "channelId", Speed float32 "speed", Command string "command"}`; `PtzPreset{Version, ChannelID, PresetList PresetList "presetList"}`; `PresetList{Presets []Preset "preset"}`; `Preset{ID uint8 "id", Name string "name,omitempty", Command string "command,omitempty"}`; `PtzZoomFocus{Version, ChannelID, Zoom, Focus ZoomPos}`; `ZoomPos{Max "maxPos", Min "minPos", Cur "curPos" uint32}`; `StartZoomFocus{Version, ChannelID, Command "command", MovePos uint32 "movePos"}`; `Support{… Items []SupportItem "item"}` with `SupportItem{ChnID uint32 "chnID", PtzType *uint32 "ptzType"}` — field names/tags copied from `../neolink/crates/core/src/bc/xml.rs`
  - `func (c *Client) PTZMove(dir Direction, speed float32) error` — msg 18, class `ClassModern24`, channel from cfg
  - `func (c *Client) PTZPresets() ([]Preset, error)` — msg 190
  - `func (c *Client) PTZGotoPreset(id uint8) error` — msg 19 `toPos`
  - `func (c *Client) PTZSetPreset(id uint8, name string) error` — msg 19 `setPos`
  - `func (c *Client) ZoomFocus() (*PtzZoomFocus, error)` — msg 294
  - `func (c *Client) ZoomTo(pos uint32) error` — msg 295 `zoomPos`, pos clamped to the last known min/max by the caller
  - `func (c *Client) PTZCaps() (PTZCaps, error)` with `type PTZCaps struct { PanTilt, Zoom bool; ZoomMin, ZoomMax uint32 }`: PanTilt = support item for this channel has `ptzType` ≠ 0; Zoom = msg 294 answers 200 with `Max > Min`. A 294 error/non-200 means `Zoom=false`, not an error.
  - every request: non-200 response → `error` containing the msg id and code; reply timeout = `cfg.ReplyTimeout`
- Fake camera: answers 18/19/190/199/294/295 with 200 by default, keeps presets in memory (190 lists them, 19 `setPos` adds/renames, 19 `toPos` on unknown id → 400), records `"ptz:<command>:<speed>"`, `"preset:<cmd>:<id>"`, `"zoom:<pos>"` in `events()`. Options on the fake: `ptzType` (default 1) and zoom range (default 0..33, cur 0); a fake with `ptzType 0` and 294 → 400 models a non-PTZ camera.

- [ ] **Step 1: Write failing tests** in `pkg/bc/ptz_msg_test.go`:
  - `TestPTZMoveSendsControl`: `PTZMove(DirLeftUp, 32)` then `PTZMove(DirStop, 0)` → events `["ptz:leftUp:32", "ptz:stop:0"]`; decoded request XML has `<channelId>0</channelId>`.
  - `TestPTZPresetsRoundTrip`: `PTZSetPreset(3,"door")` → `PTZPresets()` returns `[{ID:3,Name:"door"}]`; `PTZGotoPreset(3)` → event `"preset:toPos:3"`; `PTZGotoPreset(9)` → error mentioning `19` and `400`.
  - `TestPTZCaps`: default fake → `{PanTilt:true, Zoom:true, ZoomMin:0, ZoomMax:33}`; non-PTZ fake → `{false,false,0,0}` and `err == nil`.
  - `TestZoomTo`: `ZoomTo(20)` → event `"zoom:20"`, then `ZoomFocus().Zoom.Cur == 20`.
- [ ] **Step 2: Run** `go test -race ./pkg/bc -run 'PTZ|Zoom' -v` — FAIL (undefined).
- [ ] **Step 3: Implement** the consts, XML types, `Body` fields (`PtzControl`, `PtzPreset`, `PtzZoomFocus`, `StartZoomFocus`, `Support`) and the methods in `ptz_msg.go`, following `getTalkAbility` in `client.go` (subscribeNew → send with `NewExtension` channel → `recv`). Extend the fake camera.
- [ ] **Step 4: Run** `go test -race ./pkg/bc/...` — all PASS (old tests unchanged).
- [ ] **Step 5: Hand back for commit** `bc: PTZ, preset, zoom and support requests`.

### Task 2: Per-camera PTZ controller with session reuse and safety stop

**Files:**
- Create: `pkg/bc/ptz.go`
- Modify: `pkg/bc/bc.go` (extract the connect part of `dial` into `connect`), `pkg/bc/producer.go` (register/unregister the producer's client)
- Test: `pkg/bc/ptz_test.go`

**Interfaces:**
- Consumes: Task 1 methods and `PTZCaps`.
- Produces:
  - `func connect(rawURL string, tune func(*udp.Options)) (conn io.ReadWriteCloser, cfg Config, kind StreamKind, protocol string, err error)` — exactly today's parsing/dial/discovery in `dial`; `dial` becomes `connect` + `NewProducer` + `Protocol`. Same errors as today.
  - `func CameraKey(rawURL string) (string, error)` — `lower(hostname) + "/" + channel` (port ignored; UID and IP of the same camera are different keys — accepted).
  - registry: `NewProducer` calls `registerClient(key, client)` after a successful start; removed when `client.Done()` closes. `func activeClient(key string) *Client` returns any live one (most recently registered).
  - `func PTZFor(rawURL string) (*PTZ, error)` — one `*PTZ` per `CameraKey`, cached for the process; holds the rawURL of the first caller for control sessions.
  - `type Velocity struct { Pan, Tilt, Zoom float64; HasPanTilt, HasZoom bool }` (ONVIF −1..1, +Tilt = up, +Zoom = in)
  - `func (p *PTZ) Caps(ctx context.Context) (PTZCaps, error)` — cached after first success; concurrent callers share one fetch.
  - `func (p *PTZ) Move(v Velocity, timeout time.Duration) error` — pan/tilt: direction from signs with dead zone `|x| < 0.05` → 0; both 0 → `DirStop`; speed `= clamp(round(max(|pan|,|tilt|)*64), 1, 64)`; NaN/±Inf treated as 0, values clamped to ±1. Zoom (if `HasZoom` and ≠ 0): start a stepper that every 200 ms moves `ZoomTo(cur ± max(1, round((max−min)/20 * |zoom|)))` clamped to [min,max] until Stop, the limit, or the timeout. A new Move replaces the previous one. `timeout <= 0` or > 10 s → 10 s. When the timer fires: send `DirStop` (pan/tilt) and stop the stepper.
  - `func (p *PTZ) Stop(panTilt, zoom bool) error`
  - `func (p *PTZ) Presets() ([]Preset, error)`; `func (p *PTZ) GotoPreset(id uint8) error`; `func (p *PTZ) SetPreset(id int, name string) (uint8, error)` — `id < 0` → lowest unused id in 0..63, else error `"bc: no free preset slot"`.
  - Client selection for every call: `activeClient(key)`; else the control session: one per `PTZ`, opened with `connect` + `NewClient` + `Login` (no stream), single-flight under concurrency, closed after `controlIdle = 60 * time.Second` without use, reopened on demand. A control session that dies is dropped and reopened at most once per call; P2P `CooldownError` is returned as-is (no loop).
- [ ] **Step 1: Write failing tests** in `pkg/bc/ptz_test.go` (fake camera over TCP):
  - `TestPTZVelocityMapping`: table — `{1,0}`→`right:64`, `{-0.5,0.5}`→`leftUp:32`, `{0,-0.02}`→`stop`, `{NaN,2}`→`up:64`, `{0.001,0.001}`→`stop`.
  - `TestPTZSafetyStop`: `Move({Pan:1,HasPanTilt:true}, 300*time.Millisecond)` with no Stop → events end with `ptz:stop:0` within 1 s; `Move(..., 0)` uses 10 s (assert via an injectable `maxMove` var set to 400 ms in the test).
  - `TestPTZReusesProducerClient`: start a `Producer` on the fake, then `PTZFor(sameURL+"&stream=sub")` and `Move` → fake sees exactly one login.
  - `TestPTZControlSessionSingleFlight`: no producer; 10 goroutines call `Presets()` at once → exactly one login; after `controlIdle` (test-set to 200 ms) the connection is closed; next call logs in again.
  - `TestPTZZoomStepper`: zoom range 0..33, `Move({Zoom:1,HasZoom:true}, 2*time.Second)`, Stop after 500 ms → zoom events strictly increasing, last ≤ 33, no zoom events after Stop.
  - `TestPTZSetPresetFreeSlot`: presets {0,1,3} exist → `SetPreset(-1,"x")` returns 2.
  - Existing tests for `Dial` still pass unchanged (extraction check).
- [ ] **Step 2: Run** `go test -race ./pkg/bc -run 'PTZ' -v` — FAIL.
- [ ] **Step 3: Implement** `connect`, registry, `PTZ` per the interface above.
- [ ] **Step 4: Run** `go test -race ./pkg/bc/...` — all PASS, no races.
- [ ] **Step 5: Hand back for commit** `bc: per-camera PTZ controller with session reuse and safety stop`.

### Task 3: ONVIF PTZ envelopes, parsers and SOAP fault

**Files:**
- Modify: `pkg/onvif/server.go`
- Create: `pkg/onvif/ptz.go`
- Test: `pkg/onvif/ptz_test.go`

**Interfaces:**
- Produces (package `onvif`):
  - consts `PTZGetConfigurations="GetConfigurations"`, `PTZGetConfiguration="GetConfiguration"`, `PTZGetConfigurationOptions="GetConfigurationOptions"`, `PTZGetNodes="GetNodes"`, `PTZGetNode="GetNode"`, `PTZGetPresets="GetPresets"`, `PTZGotoPreset="GotoPreset"`, `PTZSetPreset="SetPreset"`, `PTZRemovePreset="RemovePreset"`, `PTZContinuousMove="ContinuousMove"`, `PTZStop="Stop"`, `PTZGetStatus="GetStatus"`; `PathPTZ = "/onvif/ptz_service"`
  - `type PTZFeatures struct { PanTilt, Zoom bool }`
  - `GetCapabilitiesResponse(host string, ptz bool) []byte`, `GetServicesResponse(host string, ptz bool) []byte` — with `ptz=false` output byte-identical to today; `ptz=true` adds the PTZ XAddr (`http://host/onvif/ptz_service`, namespace `http://www.onvif.org/ver20/ptz/wsdl`).
  - `GetProfilesResponse(names []string, ptz map[string]PTZFeatures) []byte`, `GetProfileResponse(name string, ptz *PTZFeatures) []byte` — a profile with features gets `<tt:PTZConfiguration token="ptz_<name>">` with `NodeToken="ptz_node_<name>"`, `DefaultContinuousPanTiltVelocitySpace` (`http://www.onvif.org/ver10/tptz/PanTiltSpaces/VelocityGenericSpace`) only if PanTilt, `DefaultContinuousZoomVelocitySpace` (`…/ZoomSpaces/VelocityGenericSpace`) only if Zoom, `DefaultPTZSpeed`, `DefaultPTZTimeout` `PT10S`; nil/absent features → byte-identical to today.
  - `PTZConfigurationsResponse(name string, f PTZFeatures)`, `PTZConfigurationResponse(...)`, `PTZConfigurationOptionsResponse(f)` (continuous spaces only, x/y/zoom range −1..1, timeout `PT1S`..`PT10S`), `PTZNodesResponse(name, f, maxPresets int)`, `PTZNodeResponse(...)`, `PTZServiceCapabilitiesResponse()` (`MoveStatus="false" StatusPosition="false"`), `PTZPresetsResponse(presets []PTZPreset)` with `type PTZPreset struct{ Token, Name string }`, `PTZSetPresetResponse(token string)`, `PTZEmptyResponse(operation string)` for GotoPreset/ContinuousMove/Stop.
  - `FaultResponse(subcode, reason string) []byte` — SOAP 1.2 `env:Fault`, `env:Receiver`/`ter:<subcode>`; callers send it with HTTP 500.
  - `type ContinuousMoveRequest struct{ Token string; PanTilt struct{X,Y float64}; Zoom float64; HasPanTilt, HasZoom bool; Timeout time.Duration }`; `ParseContinuousMove(b []byte) (ContinuousMoveRequest, error)` — reads `Velocity/PanTilt@x,y`, `Velocity/Zoom@x`, ISO-8601 `Timeout` (`PT0.5S`, `PT10S`, `PT1M`); `ParseStop(b) (token string, panTilt, zoom bool)` (absent flags = true); `ParsePreset(b) (token, presetToken, presetName string)`.
- [ ] **Step 1: Write failing tests** in `pkg/onvif/ptz_test.go`, using request bodies as zeep sends them (namespaced `ns0:`, attributes, and the un-prefixed form):
  - `TestCapabilitiesUnchangedWithoutPTZ`: `GetCapabilitiesResponse(h,false)` and `GetServicesResponse(h,false)` equal golden strings captured from the current code before the change; same for `GetProfilesResponse(names, nil)`.
  - `TestProfileWithPTZ`: features `{true,true}` → contains both velocity spaces and `token="ptz_cam"`; `{true,false}` → no zoom space.
  - `TestParseContinuousMove`: Frigate-style body (`PanTilt x="-0.5" y="0.5"`, no Zoom, no Timeout) → `{X:-0.5,Y:0.5,HasPanTilt:true,HasZoom:false,Timeout:0}`; zoom-only body → `HasPanTilt:false, Zoom:1`; `PT0.5S` → 500 ms; garbage → error.
  - `TestParseStopDefaults`: body without flags → `panTilt,zoom == true,true`.
  - `TestFaultIsSOAP`: contains `env:Fault` and the reason; parses as XML.
  - every response parses as XML (`xml.Unmarshal` into a generic struct).
- [ ] **Step 2: Run** `go test ./pkg/onvif -v` — FAIL.
- [ ] **Step 3: Implement** in the style of `server.go` (`NewEnvelope`, `Appendf`). Update callers in `internal/onvif` to pass `false`/`nil` so behaviour is unchanged until Task 4.
- [ ] **Step 4: Run** `go test ./pkg/onvif/... && go build ./...` — PASS.
- [ ] **Step 5: Hand back for commit** `onvif: PTZ service envelopes, request parsing and SOAP faults`.

### Task 4: Wire ONVIF PTZ to `bc://` streams

**Files:**
- Modify: `internal/onvif/onvif.go`, `internal/bc/bc.go`
- Create: `internal/onvif/ptz.go`
- Test: `internal/onvif/ptz_test.go`

**Interfaces:**
- Consumes: Task 2 `PTZFor`, `(*PTZ).Caps/Move/Stop/Presets/GotoPreset/SetPreset`; Task 3 builders/parsers; `streams.Get(name).Sources()`, `streams.GetAllNames()`.
- Produces:
  - `func bcSource(name string) string` — first source of stream `name` starting with `bc://`, else `""`.
  - `func hasBCStreams() bool` — any stream has a bc source; passed as `ptz` to Get{Capabilities,Services}Response.
  - `func ptzFeatures(names []string, wait time.Duration) map[string]PTZFeatures` — concurrent `Caps` per bc stream with a shared deadline `wait = 8 * time.Second`; streams whose caps did not arrive in time are omitted (fetch keeps running and fills the cache).
  - preset tokens on the wire are the decimal Baichuan id (`"3"`); names as the camera reports them (UTF-8).
  - Routing: in `onvifDeviceService`, when `r.URL.Path == onvif.PathPTZ` dispatch to `ptzService(w, r, operation, body)`; `GetServiceCapabilities` on that path returns `PTZServiceCapabilitiesResponse`. Errors → `FaultResponse` + HTTP 500; unknown token / no bc source → fault `ter:NoProfile`; unknown preset → `ter:NoToken`; `RemovePreset` → fault `ter:ActionNotSupported`; `GetStatus` → fault `ter:ActionNotSupported`.
  - `internal/bc.Init`: after streams load, start one goroutine that calls `Caps` for every bc stream (prefetch, errors logged at debug).
- [ ] **Step 1: Write failing tests** in `internal/onvif/ptz_test.go` using `httptest` against `onvifDeviceService` with a stream whose sources are `["ffmpeg:x#audio=opus", "bc://u:p@<fake addr>"]` (fake camera from a small exported test helper or a stub `PTZ` behind an interface — builder's choice, but the handler must be tested end-to-end over HTTP):
  - `TestPTZProfileAdvertised`: GetProfiles → profile for that stream has PTZConfiguration; a second stream `rtsp://…` has none.
  - `TestPTZContinuousMoveAndStop`: ContinuousMove `x=1,y=0` → camera event `ptz:right:64`; Stop → `ptz:stop:0`.
  - `TestPTZPresets`: SetPreset name "door" → token returned; GetPresets lists it; GotoPreset with it → `preset:toPos:<id>`; GotoPreset `"99"` → HTTP 500 + `NoToken` fault.
  - `TestPTZNoBCStreams`: only `rtsp://` streams → GetCapabilities byte-identical to today's golden.
  - `TestPTZProfilesWaitBounded`: a bc source that never answers → GetProfiles returns within 9 s with that stream without PTZConfiguration.
- [ ] **Step 2: Run** `go test -race ./internal/onvif -v` — FAIL.
- [ ] **Step 3: Implement** as per Interfaces.
- [ ] **Step 4: Run** `go test -race ./internal/onvif/... ./pkg/... && go build ./...` — PASS.
- [ ] **Step 5: Hand back for commit** `onvif: PTZ for bc:// streams`.

### Task 5: Docs, Frigate-client check and live verification

**Files:**
- Modify: `internal/bc/README.md` (new section "PTZ (Frigate, Home Assistant)"), `internal/onvif/README.md` (PTZ service), `.github/release-notes/` only if a release is cut (not part of this task).

- [ ] **Step 1: Docs.** Frigate example exactly:
  ```yaml
  cameras:
    e1-zoom:
      onvif:
        host: 127.0.0.1
        port: 1984
        profile: e1-zoom        # the go2rtc stream name
  ```
  plus: manual PT, zoom (stepped, say so if that is the outcome), presets (create in the Reolink app or via ONVIF SetPreset), no autotracking, camera's own ONVIF/RTSP can stay off, works over UID.
- [ ] **Step 2 (orchestrator): Frigate-client check.** In the scratchpad, `python3 -m venv` + `pip install onvif-zeep-async` (the library Frigate 0.18.2 imports as `onvif`), run go2rtc with a fake-camera stream, and replay Frigate's init sequence from `frigate/ptz/onvif.py` (`update_xaddrs`, `create_media_service`, `GetProfiles`, profile selection by token, `create_ptz_service`, `GetConfigurationOptions`, `GetPresets`, `ContinuousMove`, `Stop`, `GotoPreset`). Expected: features `pt`, `zoom`; no `pt-r`, `pt-r-fov`, `zoom-a`.
- [ ] **Step 3 (orchestrator): Live E1 Zoom.** Credentials from the maintainer, kept in a scratchpad config only. Over TCP (`bc://…@192.168.1.50`) and by UID: each direction, stop, safety stop, zoom in/out to both limits, list presets, record existing ones, SetPreset into a free slot, GotoPreset between two, restore/leave state as recorded. Probe whether `PtzControl` with `command` `zoomIn`/`zoomOut` moves the lens; if it does, open a follow-up to replace the stepper (not in this plan).
- [ ] **Step 4: Hand back for commit** `docs: PTZ through go2rtc's ONVIF server`.
