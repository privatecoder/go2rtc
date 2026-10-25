# ONVIF

## ONVIF Client

[`new in v1.5.0`](https://github.com/AlexxIT/go2rtc/releases/tag/v1.5.0)

The source is not very useful if you already know RTSP and snapshot links for your camera. But it can be useful if you don't.

**WebUI > Add** webpage supports ONVIF autodiscovery. Your server must be on the same subnet as the camera. If you use Docker, you must use "network host".

```yaml
streams:
  dahua1: onvif://admin:password@192.168.1.123
  reolink1: onvif://admin:password@192.168.1.123:8000
  tapo1: onvif://admin:password@192.168.1.123:2020
```

## ONVIF Server

A regular camera has a single video source (`GetVideoSources`) and two profiles (`GetProfiles`).

Go2rtc has one video source and one profile per stream.

### PTZ

[`bc://` streams](../bc/README.md#ptz-frigate-home-assistant) of cameras with pan/tilt or zoom get a PTZ service at `/onvif/ptz_service`.
It is advertised in `GetCapabilities` and `GetServices` only if a `bc://` stream exists, so other installs answer as before. A profile
has a `PTZConfiguration` only if its stream's first `bc://` source is a camera that can move; the profile token is the stream name.

| operation                                                   | answer                                                              |
|-------------------------------------------------------------|---------------------------------------------------------------------|
| `GetServiceCapabilities`, `GetConfigurations`, `GetConfiguration`, `GetConfigurationOptions`, `GetNodes`, `GetNode` | continuous pan/tilt and zoom velocity only |
| `ContinuousMove` (optional `Timeout`), `Stop`               | move and stop, a move without `Stop` ends after 10 seconds at most  |
| `GetPresets`, `GotoPreset`, `SetPreset`                     | the camera's presets, tokens are its preset numbers                 |
| `RemovePreset`, `GetStatus`, `AbsoluteMove`, `RelativeMove` and everything else | SOAP fault `ter:ActionNotSupported`                  |

Faults are SOAP 1.2 with HTTP status 500: `ter:NoProfile` (no such stream, or it has no `bc://` source), `ter:NoToken` (not a preset
number), `ter:NoConfig` and `ter:NoEntity` (unknown configuration or node), `ter:InvalidArgVal` (bad velocity, timeout or flag), and
`ter:Action` with the text "camera not reachable" when the camera does not take the command.

### Authentication

Without `api: username` the ONVIF server has no authentication, as before. With `api: username/password` set, the ONVIF user and password are the
API username and password, and `/onvif/` no longer uses the HTTP Basic check of the other paths (every other path keeps it, including any path the web UI is
moved to below `/onvif/` with `base_path`). A request is
accepted with

- a WS-Security `UsernameToken` in the SOAP header (`Envelope > Header > Security`, one token, user and password compared exactly), as ONVIF clients send it (Frigate, Home Assistant): `PasswordDigest` (with a
  `Created` time within 5 minutes of the server's clock) or `PasswordText`, or
- HTTP Basic auth with the same username and password.

Requests from `127.0.0.1` need no credentials, unless `local_auth: true` is set (the same rule as for the API). The credentials are removed from the request before it is written to the log. A request without valid
credentials gets HTTP 400 and a SOAP fault `env:Sender` / `ter:NotAuthorized` ("Sender not authorized").

`GetSystemDateAndTime`, `GetCapabilities`, `GetServices`, `GetServiceCapabilities` and `GetHostname` of the device and media services
are answered without credentials, as the ONVIF specification asks, because clients call them before they log in. Everything else, and
every request to the PTZ service, needs them. A nonce is not remembered, so a captured request can be replayed for 5 minutes; use it
on a trusted network, or over HTTPS (`api: tls_listen`).

## Tested clients

Go2rtc works as ONVIF server:

- Happytime onvif client (windows)
- Home Assistant ONVIF integration (linux)
- Onvier (android)
- ONVIF Device Manager (windows)

PS. Supports only TCP transport for RTSP protocol. UDP and HTTP transports - unsupported yet.

## Tested cameras

Go2rtc works as ONVIF client:

- Dahua IPC-K42
- OpenIPC
- Reolink RLC-520A
- TP-Link Tapo TC60
