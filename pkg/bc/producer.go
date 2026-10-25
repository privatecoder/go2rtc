package bc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

// These are variables only so that tests do not have to wait for them.
var (
	probeVideoTimeout = 15 * time.Second // first keyframe with parameter sets
	probeAudioTimeout = 2 * time.Second  // audio after the keyframe, cameras without mic never send any
	idleTimeout       = 15 * time.Second // no usable media while connected
	stopFlushTimeout  = 500 * time.Millisecond
)

const (
	adpcmSampleRate = 8000 // the stream is always 8 kHz, whatever talk uses

	aacPayloadType = 96 // dynamic, the RTSP consumer assigns its own anyway

	// a replacement producer starts this much after the end of the previous one
	timelineGap = 1000 // µs
)

// go2rtc adds backchannel tracks through core.Consumer (streams.Producer.AddTrack).
var (
	_ core.Producer = (*Producer)(nil)
	_ core.Consumer = (*Producer)(nil)
)

// ErrFormatChanged is returned by Start when the camera changes codec,
// parameter sets or audio format mid-stream: the consumers were set up for
// the old format, go2rtc reconnects and probes again.
var ErrFormatChanged = errors.New("bc: video format changed")

// The RTP timeline of a source comes from the process clock, and what a
// producer hands out is remembered per source (the URL), so that a replacement
// producer (go2rtc reconnects and moves the consumers over) continues after the
// one it replaces instead of starting at 0 again: timestamps and, because the
// consumers keep one RTP session, the AAC sequence numbers too. One camera's
// clock never moves another camera's timeline.
var (
	processStart = time.Now()

	sourcesMu sync.Mutex
	sources   = map[string]*sourceState{}
)

type sourceState struct {
	endUS atomic.Uint64 // µs, the furthest timestamp handed out
	seq   atomic.Uint32 // last RTP sequence number of the audio packets
}

func sourceOf(key string) *sourceState {
	sourcesMu.Lock()
	defer sourcesMu.Unlock()
	s := sources[key]
	if s == nil {
		s = new(sourceState)
		sources[key] = s
	}
	return s
}

type audioKind uint8

const (
	audioNone audioKind = iota
	audioAAC
	audioADPCM
)

type Producer struct {
	core.Connection

	client *Client
	stream *Stream

	// frames read while probing, delivered first by Start
	pending []*Media

	video, audio *core.Receiver
	videoCodec   string // "H264" or "H265" as in BcMedia
	hevc         bool
	params       map[byte][]byte // current parameter sets by NAL type
	needKey      bool            // video is held back until a good keyframe

	audioKind   audioKind
	audioFormat uint32 // AAC: profile, rate and channels of the probed stream

	src *sourceState

	based  bool
	baseUS uint64 // where this producer's camera clock starts on the process timeline

	talkCfg  *TalkConfig // nil if the camera has no usable talk
	hub      *talkHub
	talkSent atomic.Int64 // bytes of talk blocks sent
}

// NewProducer logs in over conn, starts the stream and probes it. On error
// the connection is closed.
func NewProducer(conn io.ReadWriteCloser, cfg Config, kind StreamKind, source string) (*Producer, error) {
	client := NewClient(conn, cfg)
	if _, err := client.Login(); err != nil {
		_ = client.Close()
		return nil, err
	}

	// before the stream, so its frames do not queue up meanwhile
	talkCfg := probeTalk(client)

	stream, err := client.StartStream(kind)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	p := &Producer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "bc",
			Protocol:   "tcp",
			Source:     source,
			Transport:  client,
		},
		client:  client,
		stream:  stream,
		talkCfg: talkCfg,
	}
	p.hub = &talkHub{p: p}
	p.src = sourceOf(source)
	if rc, ok := conn.(interface{ RemoteAddr() net.Addr }); ok && rc.RemoteAddr() != nil {
		p.RemoteAddr = rc.RemoteAddr().String()
	}

	if err = p.probe(); err != nil {
		_ = stream.Stop(0)
		_ = client.Close()
		return nil, err
	}

	if talkCfg != nil {
		p.Medias = append(p.Medias, talkMedia())
	}
	if key, err := CameraKey(source); err == nil {
		registerClient(key, client)
	}
	return p, nil
}

func (p *Producer) probe() error {
	timer := time.NewTimer(probeVideoTimeout)
	defer timer.Stop()

	for {
		select {
		case m, ok := <-p.stream.C:
			if !ok {
				if err := p.client.Err(); err != nil {
					return err
				}
				return io.EOF
			}
			if p.video == nil {
				if !p.probeVideo(m) {
					p.probeAudio(m)
					continue
				}
				// the keyframe starts the buffer, audio seen before it is not kept
				p.pending = append(p.pending, m)
				if p.audio != nil {
					return nil
				}
				timer.Reset(probeAudioTimeout)
				continue
			}
			p.pending = append(p.pending, m)
			if p.probeAudio(m) {
				return nil
			}

		case <-timer.C:
			if p.video == nil {
				return errors.New("bc: probe: no keyframe with parameter sets")
			}
			return nil // no audio
		}
	}
}

// nalUnit is one NAL unit of an AVCC access unit, without its length prefix.
type nalUnit struct {
	typ  byte
	data []byte
}

// splitAVCC splits an AVCC access unit and checks it completely: every NAL
// has its full length and at least its header (1 byte for H264, 2 for H265).
// The helpers in pkg/h264 and pkg/h265 and the consumers index into the data
// without checks, so nothing that fails here may reach them.
func splitAVCC(b []byte, hevc bool) ([]nalUnit, bool) {
	hdr := 1
	if hevc {
		hdr = 2
	}

	var nals []nalUnit
	for len(b) > 0 {
		if len(b) < 4+hdr {
			return nil, false
		}
		size := int(binary.BigEndian.Uint32(b))
		if size < hdr || size > len(b)-4 {
			return nil, false
		}
		data := b[4 : 4+size]
		typ := data[0] & 0x1F
		if hevc {
			typ = (data[0] >> 1) & 0x3F
		}
		nals = append(nals, nalUnit{typ, data})
		b = b[4+size:]
	}
	return nals, len(nals) > 0
}

// Parameter set sizes below these are nonsense and some helper indexes into
// them: h264.AVCCToCodec reads bytes 1..3 of the SPS (profile, constraints,
// level), h265.DecodeSPS skips the 2 byte NAL header.
const (
	minH264SPS = 4
	minH264PPS = 2
	minH265PS  = 4 // VPS, SPS and PPS
)

// hasVCL reports whether the access unit carries picture data. Without it
// there is nothing to show, and h264.RepairAVCC strips a leading SEI and
// would then index into an empty payload.
func hasVCL(nals []nalUnit, hevc bool) bool {
	for _, n := range nals {
		if hevc && n.typ < 32 || !hevc && n.typ >= 1 && n.typ <= 5 {
			return true
		}
	}
	return false
}

// paramTypes are the NAL types that make up the video configuration.
func paramTypes(hevc bool) []byte {
	if hevc {
		return []byte{h265.NALUTypeVPS, h265.NALUTypeSPS, h265.NALUTypePPS}
	}
	return []byte{h264.NALUTypeSPS, h264.NALUTypePPS}
}

func isParam(typ byte, hevc bool) bool {
	for _, t := range paramTypes(hevc) {
		if t == typ {
			return true
		}
	}
	return false
}

// canonicalSets is the current parameter sets as AVCC, in the order a
// decoder wants them (VPS, SPS, PPS).
func canonicalSets(params map[byte][]byte, hevc bool) []byte {
	var sets [][]byte
	for _, typ := range paramTypes(hevc) {
		sets = append(sets, params[typ])
	}
	return h264.JoinNALU(sets...)
}

func paramOK(typ byte, n int, hevc bool) bool {
	switch {
	case hevc:
		return n >= minH265PS
	case typ == h264.NALUTypeSPS:
		return n >= minH264SPS
	}
	return n >= minH264PPS
}

// probeVideo builds the video codec from a keyframe that carries its
// parameter sets and reports whether it did.
func (p *Producer) probeVideo(m *Media) bool {
	if m.Kind != KindIframe || (m.Codec != "H264" && m.Codec != "H265") {
		return false
	}
	hevc := m.Codec == "H265"

	avcc := annexb.EncodeToAVCC(m.Data)
	nals, ok := splitAVCC(avcc, hevc)
	if !ok || !hasVCL(nals, hevc) {
		return false
	}

	// every occurrence is checked, the helpers read all of them
	params := map[byte][]byte{}
	for _, n := range nals {
		if !isParam(n.typ, hevc) {
			continue
		}
		if !paramOK(n.typ, len(n.data), hevc) {
			return false
		}
		if params[n.typ] == nil {
			params[n.typ] = n.data
		}
	}
	if len(params) != len(paramTypes(hevc)) {
		return false
	}

	// the codec is made from the validated sets, not from the untrusted unit
	canon := canonicalSets(params, hevc)
	var codec *core.Codec
	if hevc {
		codec = h265.AVCCToCodec(canon)
	} else {
		codec = h264.AVCCToCodec(canon)
	}

	p.videoCodec, p.hevc, p.params = m.Codec, hevc, params
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		Codecs:    []*core.Codec{codec},
	}
	p.Medias = append(p.Medias, media)
	p.video = core.NewReceiver(media, codec)
	p.Receivers = append(p.Receivers, p.video)
	return true
}

// probeAudio builds the audio codec from the first AAC or ADPCM frame.
func (p *Producer) probeAudio(m *Media) bool {
	if p.audio != nil {
		return true
	}

	var codec *core.Codec
	switch m.Kind {
	case KindAAC:
		info, ok := parseADTS(m.Data)
		if !ok {
			return false
		}
		if codec = aac.ADTSToCodec(m.Data); codec == nil {
			return false
		}
		// standard RFC 3640 packets (what RTSP cameras send), see writeAAC
		codec.PayloadType = aacPayloadType
		p.audioKind, p.audioFormat = audioAAC, info.format
	case KindADPCM:
		// decoded to PCM and sent as G.711 A-law, which every consumer takes
		codec = &core.Codec{Name: core.CodecPCMA, ClockRate: adpcmSampleRate}
		p.audioKind = audioADPCM
	default:
		return false
	}

	media := &core.Media{
		Kind:      core.KindAudio,
		Direction: core.DirectionRecvonly,
		Codecs:    []*core.Codec{codec},
	}
	p.Medias = append(p.Medias, media)
	p.audio = core.NewReceiver(media, codec)
	p.Receivers = append(p.Receivers, p.audio)
	return true
}

// Start forwards the stream until it fails. On every return the BC client is
// closed (the camera session must not outlive the producer while go2rtc is
// busy replacing it), but the receivers stay: go2rtc moves them to the
// replacement.
func (p *Producer) Start() error {
	defer p.release()

	pending := p.pending
	p.pending = nil
	for _, m := range pending {
		if _, err := p.handle(m); err != nil {
			p.client.cfg.Log.Warnf("%v, reconnecting", err)
			return err
		}
	}

	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()

	for {
		select {
		case m, ok := <-p.stream.C:
			if !ok {
				if err := p.client.Err(); err != nil {
					return err
				}
				return io.EOF
			}
			useful, err := p.handle(m)
			if err != nil {
				p.client.cfg.Log.Warnf("%v, reconnecting", err)
				return err
			}
			// only media that was forwarded shows the stream is alive; with video
			// in the stream that is video, audio alone must not hide a video that
			// stalled or whose keyframes are all damaged
			if useful && (p.video == nil || m.Kind == KindIframe || m.Kind == KindPframe) {
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
				idle.Reset(idleTimeout)
			}

		case <-idle.C:
			return fmt.Errorf("bc: no usable media for %s", idleTimeout)
		}
	}
}

// release ends talk and the camera connection after Start.
func (p *Producer) release() {
	p.stopTalk(0)
	_ = p.client.Close()
}

// Stop cancels at once. The goodbye to the camera (stream stop, talk reset)
// is best effort within a short budget and never delays the close.
func (p *Producer) Stop() error {
	p.stopTalk(300 * time.Millisecond)
	_ = p.stream.Stop(0)
	p.client.Flush(stopFlushTimeout)
	return p.Connection.Stop()
}

// handle forwards one frame. useful says whether it counts as proof of life.
// An error means the format changed and the producer cannot continue.
func (p *Producer) handle(m *Media) (useful bool, err error) {
	switch m.Kind {
	case KindIframe, KindPframe:
		if p.video == nil {
			return false, nil
		}
		return p.handleVideo(m)

	case KindAAC:
		if p.audio == nil {
			return false, nil
		}
		if p.audioKind != audioAAC {
			return false, errors.New("bc: audio format changed")
		}
		return p.writeAAC(m)

	case KindADPCM:
		if p.audio == nil {
			return false, nil
		}
		if p.audioKind != audioADPCM {
			return false, errors.New("bc: audio format changed")
		}
		return p.writeADPCM(m)
	}
	return false, nil
}

func (p *Producer) handleVideo(m *Media) (bool, error) {
	if m.Codec != p.videoCodec {
		return false, fmt.Errorf("%w: %s to %s", ErrFormatChanged, p.videoCodec, m.Codec)
	}

	avcc := annexb.EncodeToAVCC(m.Data)
	nals, ok := splitAVCC(avcc, p.hevc)
	if !ok || !hasVCL(nals, p.hevc) {
		// damaged or empty: nothing from here until a good keyframe
		p.needKey = true
		return false, nil
	}

	if m.Kind == KindIframe {
		// every parameter set in it must be sane, and must stay what the
		// consumers were set up with
		have := map[byte]bool{}
		for _, n := range nals {
			if !isParam(n.typ, p.hevc) {
				continue
			}
			if !paramOK(n.typ, len(n.data), p.hevc) {
				p.needKey = true
				return false, nil
			}
			if !bytes.Equal(p.params[n.typ], n.data) {
				return false, fmt.Errorf("%w: parameter set %d changed", ErrFormatChanged, n.typ)
			}
			have[n.typ] = true
		}
		if len(have) < len(paramTypes(p.hevc)) {
			avcc = p.selfContained(nals)
		}
		p.needKey = false
	} else if p.needKey {
		return false, nil
	}

	p.setBase()
	us := p.baseUS + m.PTS
	p.src.advance(us)

	p.video.WriteRTP(&rtp.Packet{
		Header:  rtp.Header{Timestamp: uint32(us * 9 / 100)}, // µs to 90 kHz
		Payload: avcc,
	})
	return true, nil
}

// selfContained puts the current parameter sets in front of a keyframe that
// lacks some of them. Consumers that were moved here from an earlier producer
// carry repair code that adds the sets they saw first to such keyframes; with
// the current ones in-band that never happens.
func (p *Producer) selfContained(nals []nalUnit) []byte {
	out := canonicalSets(p.params, p.hevc)
	for _, n := range nals {
		if !isParam(n.typ, p.hevc) {
			out = binary.BigEndian.AppendUint32(out, uint32(len(n.data)))
			out = append(out, n.data...)
		}
	}
	return out
}

// advance records how far the source's timeline has been used.
func (s *sourceState) advance(us uint64) {
	for end := s.endUS.Load(); us > end && !s.endUS.CompareAndSwap(end, us); end = s.endUS.Load() {
	}
}

// setBase puts this producer's camera clock on the source's timeline: now, but
// never before the end of what earlier producers of the source sent.
func (p *Producer) setBase() {
	if p.based {
		return
	}
	p.based = true
	p.baseUS = max(uint64(time.Since(processStart)/time.Microsecond), p.src.endUS.Load()+timelineGap)
}

// audioTS is the RTP timestamp of the audio frame's position pos on the
// timeline, in samples of the given rate.
func (p *Producer) audioTS(rate uint32, pos uint64) uint32 {
	p.setBase()
	return uint32(p.baseUS*uint64(rate)/1_000_000 + pos)
}

// audioEnd moves the timeline to the end of an audio frame, so a replacement
// producer continues after the audio too.
func (p *Producer) audioEnd(rate uint32, pos, samples uint64) {
	p.src.advance(p.baseUS + (pos+samples)*1_000_000/uint64(rate))
}

// nextSeq is the next audio RTP sequence number of the source.
func (p *Producer) nextSeq() uint16 { return uint16(p.src.seq.Add(1)) }

// writeAAC sends each ADTS frame as one standard RFC 3640 packet (AU headers
// section, one access unit), with the timestamps of the stream's audio clock.
// go2rtc's raw AAC packets would lose them in aac.RTPPay.
func (p *Producer) writeAAC(m *Media) (bool, error) {
	info, ok := parseADTS(m.Data)
	if !ok || m.AudioRate != info.rate {
		return false, nil
	}
	if info.format != p.audioFormat {
		return false, errors.New("bc: audio format changed")
	}

	pos, b := m.AudioPos, m.Data
	for _, f := range info.frames {
		au := b[f.header:f.size]
		b = b[f.size:]

		payload := make([]byte, 4+len(au))
		binary.BigEndian.PutUint16(payload, 16) // AU headers length in bits
		binary.BigEndian.PutUint16(payload[2:], uint16(len(au))<<3)
		copy(payload[4:], au)

		p.audio.WriteRTP(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				PayloadType:    aacPayloadType,
				SequenceNumber: p.nextSeq(),
				Timestamp:      p.audioTS(info.rate, pos),
			},
			Payload: payload,
		})
		pos += uint64(f.blocks) * aac.AUTime
	}
	p.audioEnd(info.rate, pos, 0)
	return true, nil
}

func (p *Producer) writeADPCM(m *Media) (bool, error) {
	samples, err := DecodeADPCM(m.Data)
	if err != nil || len(samples) == 0 {
		return false, nil
	}

	payload := make([]byte, len(samples))
	for i, s := range samples {
		payload[i] = pcm.PCMtoPCMA(s)
	}

	p.audio.WriteRTP(&rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         true,
			SequenceNumber: p.nextSeq(),
			Timestamp:      p.audioTS(adpcmSampleRate, m.AudioPos),
		},
		Payload: payload,
	})
	p.audioEnd(adpcmSampleRate, m.AudioPos, uint64(len(samples)))
	return true, nil
}
