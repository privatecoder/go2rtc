# Baichuan (Reolink)

Native client for the Reolink Baichuan protocol (the one the Reolink app and `neolink` use). It reads the camera's own
streams directly, including the third "Balanced" stream that is not available over RTSP, without any RTSP server in between.

## Features

- video H264 and H265, audio AAC (cameras with AAC) and ADPCM (converted to PCMA 8000)
- login with the encryption negotiated by the camera: none, BCEncrypt or AES
- keepalive and ping handling like `neolink`, a stalled connection is closed and go2rtc reconnects
- a slow consumer never blocks the connection: frames are dropped and video resumes at the next keyframe
- two-way audio (talk) for cameras that support it: the stream gets a backchannel (PCMA/PCMU 8000), the audio is converted to the ADPCM
  format the camera asks for and sent in real time; a talk session ends after 2 seconds without audio
- only local cameras over TCP (default port `9000`), Reolink P2P by UID is not supported yet

## Configuration

- `channel` - channel number, default `0` (only relevant for NVRs)
- `stream` - `main` (default), `sub` or `extern` (the "Balanced" stream, same as `sub` on cameras without it)
- `encryption` - the highest control channel encryption offered at login: `aes` (default), `bc` or `none`; the camera may choose a lower one
- the port can be skipped if it is the default `9000`

The Baichuan ("surveillance") port must be enabled on the camera, otherwise the connection is refused.

```yaml
streams:
  reolink:          bc://user:pass@192.168.1.123
  reolink_sub:      bc://user:pass@192.168.1.123:9000?stream=sub
  reolink_nvr_ch2:   bc://user:pass@192.168.1.10?channel=2&stream=main
  reolink_balanced: bc://user:pass@192.168.1.123?stream=extern
```

URL-encode special characters in the password, for example `@` as `%40`.

## Known limitations

- Reolink P2P (UID) and cameras behind NVR-specific handling are not supported.
- Video must carry its SPS/PPS (and VPS for H265) in the first keyframe; a camera that sends them only in separate packets is not recognized.
- If the camera changes codec, resolution or audio format while streaming, the source reconnects and probes again.
- The RTP timeline continues across reconnects of the source, but G.711 audio sent to WebRTC (for example Home Assistant) is re-timed by go2rtc's
  own G.711 repacker, which starts every consumer at zero. This is how go2rtc treats every source, the audio anchor of this source is not kept there.
- ADPCM audio from the camera is converted to PCMA 8000; AAC is passed through.
