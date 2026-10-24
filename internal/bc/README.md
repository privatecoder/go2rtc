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

Home Assistant (WebRTC camera, Frigate integration or go2rtc's own card) uses the same stream names. Define each stream once in go2rtc and let every
consumer read it from there, so the camera sees a single connection per stream.

## Two-way audio

Cameras that report a talk format get a backchannel (PCMA or PCMU, 8000 Hz) that browsers and Home Assistant use through WebRTC. The audio
is converted to the ADPCM format the camera asks for and sent in real time.

- The talk session on the camera starts with the first audio, and ends after 2 seconds of silence (a muted microphone), when the
  last bit of speech has been played out. The same microphone starts a new session when it speaks again.
- One microphone talks at a time: while one is speaking, others wait; the most recent one to speak after a pause gets the session.
- Cameras without talk (or with a talk format other than ADPCM) simply have no backchannel.

## Discovery by UID

The levels are cumulative:

- `local` - LAN broadcast with the UID (or straight to `addr`);
- `remote` - also the camera's LAN address as the Reolink servers know it;
- `map` - also a hole punch to its public address (direct P2P through NAT);
- `relay` (default) - the punch gets 3 seconds, then Reolink's relay forwards the traffic.

Needs outgoing UDP to `p2p*.reolink.com` (port 9999) and the addresses they return. The log shows the chosen path
(`media path: ...`): direct on the LAN, direct to the public address, or through the relay. The transport reports its real receive rate to
the camera in every acknowledgement, which is what lets the camera send its full bitrate.

Failed registrations are retried after 1, 2, 4 ... 30 seconds, and that delay carries over to the next connection attempt for the same
UID: Reolink limits rapid reconnects per camera, so the source does not retry faster than that. A call that would only wait out the
delay returns at once with `waiting for P2P cooldown, retry in Ns` and does not extend it.

## Notes and known limitations

- Video H264 and H265; audio AAC is passed through, ADPCM from the camera is converted to PCMA 8000.
- The first keyframe must carry the SPS/PPS (and VPS for H265); a camera that sends them only in separate packets is not recognized.
- If the camera changes codec, resolution or audio format while streaming, the source reconnects and probes again.
- Slow consumers never block the connection: frames are dropped and video resumes at the next keyframe.
- The RTP timeline continues across reconnects of the source. Like for every other source, go2rtc re-times G.711 audio for WebRTC with its
  own repacker that starts every consumer at zero.
- Log levels: discovery steps are `debug`, the chosen path and the login (with camera type and resolution) are `info`, drops, retries,
  format changes and errors are `warn`; every line starts with the UID or host.
- Not supported: PTZ, motion events, SD card playback and other camera settings.

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
