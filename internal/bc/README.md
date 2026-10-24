# Baichuan (Reolink)

Native client for the Reolink Baichuan protocol, the one the Reolink app and `neolink` speak. It reads the camera's own streams
directly, including the third "Balanced" stream that Reolink does not offer over RTSP, with no RTSP server in between. Cameras are
reached by IP over TCP, or by UID over Reolink's P2P service (UDP) when they are not directly reachable.

## Configuration

```yaml
streams:
  # by IP, TCP port 9000
  reolink:           bc://user:pass@192.168.1.123
  reolink_sub:       bc://user:pass@192.168.1.123:9000?stream=sub
  reolink_balanced:  bc://user:pass@192.168.1.123?stream=extern
  reolink_nvr_ch2:   bc://user:pass@192.168.1.10?channel=2&stream=main

  # by UID, over P2P
  reolink_uid:       bc://user:pass@95270000ABCDEFGH
  reolink_uid_sub:   bc://user:pass@95270000ABCDEFGH?stream=sub&discovery=map
  reolink_uid_lan:   bc://user:pass@95270000ABCDEFGH?discovery=local&addr=192.168.1.123
```

The Baichuan port (default `9000`, the "surveillance" or "server" port in the camera's network settings) must be enabled on the camera.
URL-encode special characters in the password, for example `@` as `%40`.

| parameter    | values                                  | meaning                                                                                                   |
|--------------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `stream`     | `main` (default), `sub`, `extern`       | the encoder stream; `extern` is the "Balanced" stream, the same as `sub` on cameras that do not have it    |
| `channel`    | number, default `0`                     | channel on an NVR; nothing else is special-cased for NVRs                                                  |
| `encryption` | `aes` (default), `bc`, `none`           | highest control-channel encryption offered at login, the camera may choose a lower one                     |
| `discovery`  | `local`, `remote`, `map`, `relay` (default) | UID sources only: how far to go, each level also tries the previous ones (see below)                   |
| `addr`       | IP address                              | UID sources only: the camera's IP if known, the LAN query goes straight to it instead of broadcasting      |
| `region`     | e.g. `Europe (Germany)`                 | UID sources only: ask a single Reolink lookup server, all are asked by default                             |

A host that is 10 or more capital letters and digits without a port is a UID, anything else is an address.

## Choosing how to connect

| situation                                              | URL                                                        | transport                   |
|--------------------------------------------------------|------------------------------------------------------------|-----------------------------|
| camera on the same LAN, fixed IP                       | `bc://user:pass@192.168.1.123`                             | TCP to port 9000            |
| camera behind a VPN or routed subnet                   | `bc://user:pass@10.8.0.50`                                 | TCP to port 9000            |
| camera on the same LAN, IP changes (DHCP)              | `bc://user:pass@95270000ABCDEFGH?discovery=local`          | UDP, LAN broadcast by UID   |
| remote camera, no VPN, NAT that allows a hole punch    | `bc://user:pass@95270000ABCDEFGH?discovery=map`            | UDP, direct P2P             |
| remote camera, any NAT                                 | `bc://user:pass@95270000ABCDEFGH`                          | UDP, direct P2P or relay    |

**By IP (TCP)** is the simplest and most robust way: use it whenever go2rtc can reach the camera's address, including through a
VPN. Nothing but the camera's port 9000 is involved.

**By UID (UDP)** finds the camera through Reolink's P2P service, the same way the Reolink app does when you are away from home:

1. `local` - a broadcast with the UID on the LAN (ports 2015 and 2018), or straight to `addr` when it is set. The camera answers
   from its current IP, so this works with DHCP.
2. `remote` - the Reolink lookup servers (`p2p*.reolink.com`, UDP 9999) tell where the camera is registered, and go2rtc tries the
   camera's LAN address as the servers know it.
3. `map` - hole punch: go2rtc and the camera send to each other's public addresses at the same time, so both NATs let the traffic
   through. The stream then flows directly between go2rtc and the camera, not through Reolink.
4. `relay` (default) - if the hole punch does not succeed within 3 seconds, Reolink's relay server forwards the traffic. This works
   behind any NAT, but every byte goes through Reolink and the bitrate may be lower.

Each level also tries the ones before it, all in parallel, and the first path that answers wins. The log shows the result at `info`:

```
95270000ABCDEFGH: media path: direct to the camera at 192.168.1.123:...           # local
95270000ABCDEFGH: media path: direct P2P to the camera's public address 1.2.3.4:...  # hole punch
95270000ABCDEFGH: media path: routed through the Reolink relay at ... (not direct)    # relay
```

Use `discovery=map` to never fall back to the relay, and `discovery=local` to keep a LAN camera away from the internet entirely.

**Requirements for UID sources**

- On the camera: UID/P2P access enabled in the network settings, and internet access for `remote`, `map` and `relay`.
- On the go2rtc host: outgoing UDP to `p2p*.reolink.com` port 9999 and to the addresses they return; nothing has to be forwarded on
  the router. In Docker, `local` needs host networking (or a macvlan network) for the LAN broadcast to reach the cameras.
- `region` (for example `Europe (Germany)`, `North America (East US)`, `Asia (Hong Kong)`) only picks which lookup server is asked;
  by default all are asked and the fastest answer is used.

## Frigate and Home Assistant

go2rtc restreams the camera, Frigate and Home Assistant read it from go2rtc:

```yaml
# go2rtc.yaml
streams:
  doorbell:     bc://user:pass@192.168.1.123?stream=main
  doorbell_sub: bc://user:pass@192.168.1.123?stream=sub
```

```yaml
# Frigate config.yml
cameras:
  doorbell:
    ffmpeg:
      inputs:
        - path: rtsp://go2rtc:8554/doorbell_sub   # detect
          input_args: preset-rtsp-restream
          roles: [detect]
        - path: rtsp://go2rtc:8554/doorbell       # record
          input_args: preset-rtsp-restream
          roles: [record]
```

Browsers play only Opus or G.711 over WebRTC, not the camera's AAC. For audio in WebRTC live views (Home Assistant, Frigate's two-way
talk) add an Opus track; go2rtc runs that ffmpeg only while a viewer needs it, and only the audio is transcoded:

```yaml
streams:
  doorbell:
    - bc://user:pass@192.168.1.123?stream=main
    - ffmpeg:doorbell#audio=opus
```

When you watch over a slow link (a VPN or mobile data), make the small stream Frigate's default live stream. A 4K main stream needs
4-5 Mbit/s; over a slower link it plays a second, then freezes while the next second arrives:

```yaml
cameras:
  doorbell:
    live:
      streams:
        Sub: doorbell_sub   # first entry is the default
        Main: doorbell
```

To use this go2rtc build in Frigate, save the binary as `/config/go2rtc` (executable, `chmod 755`) and restart Frigate; the log then
shows `Using go2rtc binary from '/config/go2rtc'`.

Home Assistant (WebRTC camera, Frigate integration or go2rtc's own card) uses the same stream names. Define each stream once in go2rtc and let every
consumer read it from there, so the camera sees a single connection per stream.

## PTZ (Frigate, Home Assistant)

go2rtc's ONVIF server offers pan, tilt, zoom and presets for `bc://` streams of cameras that have them, and turns the ONVIF calls into
Baichuan messages. Frigate and Home Assistant only speak ONVIF for PTZ, so they point at go2rtc instead of the camera, which
can keep its own ONVIF and RTSP switched off. The profile is the go2rtc stream name:

```yaml
# Frigate config.yml
cameras:
  e1-zoom:
    onvif:
      host: 127.0.0.1         # go2rtc, here inside the Frigate container
      port: 1984              # go2rtc's API port
      profile: e1-zoom        # the go2rtc stream name
```

The user and password Frigate sends are ignored. Home Assistant's ONVIF integration pointed at go2rtc gets the same PTZ.

- **Move**: pan/tilt in eight directions, speed from how far the ONVIF velocity goes (1 to 64 on the camera), and zoom. Baichuan has no
  continuous zoom, so go2rtc moves the lens in steps (every 200 ms, about a twentieth of the range per step at full speed) until
  Stop or the end of the range.
- **Stop is not left to chance**: a move without a Stop ends by itself after 10 seconds, or after the ONVIF `Timeout` of the
  request if that is shorter. A Stop the camera does not acknowledge is sent again (for up to a minute).
- **Presets**: the camera's presets are listed and can be gone to. `SetPreset` saves the current position, in the slot the client names
  or in the lowest free one of 0 to 63; presets can also be made in the Reolink app. Preset tokens are the camera's preset numbers.
  `RemovePreset` is not supported.
- **No autotracking**: only continuous movement is offered, no relative or absolute moves and no position status, so Frigate shows
  autotracking as unsupported.
- **The same for IP and UID cameras.** PTZ commands go over the session of a running stream of the camera (the main stream, or the
  `sub` stream of the same camera, whichever is up) and do not touch the video. Only when no stream of that camera is running
  go2rtc opens a session of its own for PTZ, with the same URL and discovery, and closes it after a minute without use.
- **Where PTZ shows up**: the PTZ service is advertised only if at least one `bc://` stream is configured, and a profile gets
  PTZ only if the camera reports pan/tilt or zoom (the zoom controls only if it has a zoom lens). go2rtc asks the cameras once at start
  and a client's first `GetProfiles` waits up to 8 seconds for those that have not answered; a camera that is offline has
  no PTZ in the answers until it is reachable again, it is retried after 30 seconds, then less often (up to 5 minutes), and its
  other profile data is unaffected. A stream with several sources uses its first `bc://` one.
- **No authentication**: go2rtc's ONVIF server accepts any request, whoever can reach port 1984 can move the cameras. Keep it on a
  trusted network.

When a command fails, the client gets an ONVIF fault with a general text ("camera not reachable"); the details are in go2rtc's
`onvif` log at `debug`.

## Two-way audio

Cameras that report a talk format get a backchannel (PCMA or PCMU, 8000 Hz) that browsers and Home Assistant use through WebRTC. The audio
is converted to the ADPCM format the camera asks for and sent in real time.

- The talk session on the camera starts with the first audio, and ends after 2 seconds of silence (a muted microphone), when the
  last bit of speech has been played out. The same microphone starts a new session when it speaks again.
- One microphone talks at a time: while one is speaking, others wait; the most recent one to speak after a pause gets the session.
- Cameras without talk (or with a talk format other than ADPCM) simply have no backchannel.

## Reconnects and cooldown

Failed registrations are retried after 1, 2, 4 ... 30 seconds, and that delay carries over to the next connection attempt for the same
UID: Reolink limits rapid reconnects per camera, so the source does not retry faster than that. A call that would only wait out the
delay returns at once with `waiting for P2P cooldown, retry in Ns` and does not extend it. The transport reports its real receive rate
to the camera in every acknowledgement, which is what lets the camera send its full bitrate over UDP.

## Notes and known limitations

- Video H264 and H265; audio AAC is passed through, ADPCM from the camera is converted to PCMA 8000.
- The first keyframe must carry the SPS/PPS (and VPS for H265); a camera that sends them only in separate packets is not recognized.
- If the camera changes codec, resolution or audio format while streaming, the source reconnects and probes again.
- Slow consumers never block the connection: frames are dropped and video resumes at the next keyframe.
- The RTP timeline continues across reconnects of the source. Like for every other source, go2rtc re-times G.711 audio for WebRTC with its
  own repacker that starts every consumer at zero.
- Log levels: discovery steps are `debug`, the chosen path and the login (with camera type and resolution) are `info`, drops, retries,
  format changes and errors are `warn`; every line starts with the UID or host.
- Not supported: motion events, SD card playback, PTZ calibration, focus, patrols and other camera settings. PTZ is available through
  the ONVIF server, see above.

## Troubleshooting

- `login failed, check username and password` - wrong credentials, or a camera user that is refused over Baichuan (try `admin`).
  Several failed logins in a row can lock the account for a while.
- `dial tcp ...:9000: i/o timeout` - the camera is not reachable on port 9000 (offline, wrong IP, the port disabled, or a firewall).
- Stuttering playback with a clean log: check the link between go2rtc and the viewer. To measure the camera side, record from go2rtc
  on its own host and compare the recorded duration with the wall clock (`timeout 30 ffmpeg -i rtsp://127.0.0.1:8554/<stream> -c copy
  -f mpegts out.ts`); 30 seconds of content means the camera delivers in real time.
- More detail: `log: { bc: debug }` adds the discovery steps of UID sources (TCP sources log almost nothing more at debug).

The camera's RTSP, RTMP and ONVIF services are not used by this source and can be disabled; port 9000 ("Basisdienste" / basic
service) must stay on, and HTTP is only needed for the camera's web UI and other integrations.

## Credits and license

Implemented against the protocol documentation in
[privatecoder/neolink `docs/`](https://github.com/privatecoder/neolink/tree/master/docs), written in that fork from
disassembly and packet captures of the official Reolink client and from the neolink source. Findings from that fork used here
include the meaning of the UDP ACK receive-rate field (formerly `maybe_latency`; reporting the real rate lifts the camera's
throttling), relay/lookup region selection, the A/V timestamping model and the "Balanced" third stream.

The Baichuan protocol itself (framing, login and encryption, BcMedia, UDP discovery, talk) was reverse-engineered in the
original neolink by George Hilliard, Andrew King (QuantumEntangledAndy) and contributors. Parts of `pkg/bc` are ported from
neolink, and its packet samples are used as test data.

neolink is licensed under the GNU AGPL v3; the parts of `pkg/bc` ported from it, and binaries that include them, are
distributed under the AGPL v3 as well.
