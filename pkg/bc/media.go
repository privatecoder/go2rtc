package bc

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// BcMedia frame magics. Iframe and Pframe magics carry the channel in the low
// nibble of the last byte.
const (
	magicInfoV1    = 0x31303031
	magicInfoV2    = 0x32303031
	magicIframe    = 0x63643030 // ..0x63643039
	magicIframeEnd = 0x63643039
	magicPframe    = 0x63643130 // ..0x63643139
	magicPframeEnd = 0x63643139
	magicAAC       = 0x62773530
	magicADPCM     = 0x62773130

	adpcmDataMagic = 0x0100

	mediaPad        = 8
	maxMediaPayload = 16 * 1024 * 1024
)

type MediaKind uint8

const (
	KindInfoV1 MediaKind = iota + 1
	KindInfoV2
	KindIframe
	KindPframe
	KindAAC
	KindADPCM
)

func (k MediaKind) String() string {
	switch k {
	case KindInfoV1:
		return "InfoV1"
	case KindInfoV2:
		return "InfoV2"
	case KindIframe:
		return "Iframe"
	case KindPframe:
		return "Pframe"
	case KindAAC:
		return "AAC"
	case KindADPCM:
		return "ADPCM"
	}
	return "Unknown"
}

// StreamInfo is the InfoV1/InfoV2 frame. Dates are as sent: year since 1900.
type StreamInfo struct {
	Width, Height uint32
	FPS           uint8 // an index into a table on older cameras
	Start, End    [6]uint8
}

// Media is one demuxed BcMedia frame.
type Media struct {
	Kind MediaKind

	// Info for KindInfoV1 and KindInfoV2
	Info StreamInfo

	// Video (KindIframe, KindPframe): Codec is "H264" or "H265"
	Codec        string
	Microseconds uint32 // capture timestamp, wraps every ~71 min
	Time         uint32 // POSIX seconds, only if HasTime (I-frames)
	HasTime      bool

	// Data is the Annex-B video payload, ADTS AAC, or one ADPCM block
	// (4-byte predictor header + samples).
	Data []byte

	// Timing, set by Stream for every frame before any drop decision.
	// PTS (video) is µs since the stream's first video frame, the camera's
	// u32 counter unwrapped. AudioPos (audio) is the position of the frame's
	// first sample on a timeline in AudioRate samples/s that starts at the
	// video clock when the first audio frame arrived.
	PTS       uint64
	AudioPos  uint64
	AudioRate uint32
}

// Recovery limits. Normal parsing of intact frames is purely length based;
// these only apply after a parse failure, when a four-byte magic found in the
// middle of damaged data has to be told apart from a real frame.
const (
	recoverBigFrame = 256 * 1024      // a candidate claiming more is dropped as soon as another frame start shows up inside it
	recoverMaxWait  = 2 * 1024 * 1024 // bytes buffered behind an unconfirmed incomplete candidate before it is dropped
)

// Demuxer splits the concatenated binary payloads of a video stream into
// frames. Feed it with Push, in any chunk size, and drain with Next.
//
// After garbage it resyncs to the next frame start, and only hands out a
// recovered frame once its content looks right (ADTS syncword, Annex-B start
// code, consistent ADPCM header) and the bytes after it start another frame.
type Demuxer struct {
	buf        []byte
	skipped    int
	recovering bool
}

func (d *Demuxer) Push(b []byte) { d.buf = append(d.buf, b...) }

// Buffered is the number of bytes waiting for a complete frame.
func (d *Demuxer) Buffered() int { return len(d.buf) }

// Skipped is the total number of bytes dropped while resyncing.
func (d *Demuxer) Skipped() int { return d.skipped }

var errMediaIncomplete = errors.New("bcmedia: incomplete")

// Next returns the next frame, or nil if more data is needed.
func (d *Demuxer) Next() *Media {
	for len(d.buf) > 0 {
		if !d.recovering {
			m, n, err := parseMedia(d.buf)
			switch err {
			case nil:
				d.buf = d.buf[n:]
				return m
			case errMediaIncomplete:
				return nil
			}
			d.recovering = true
			d.skip()
			continue
		}

		switch d.candidate() {
		case candWait:
			return nil
		case candReject:
			d.skip()
		case candAccept:
			m, n, _ := parseMedia(d.buf)
			d.buf = d.buf[n:]
			d.recovering = false
			return m
		}
	}
	return nil
}

// skip drops at least one byte and then everything up to the next offset that
// looks like a frame start.
func (d *Demuxer) skip() {
	for i := 1; i < len(d.buf); i++ {
		ok, more := plausibleStart(d.buf[i:])
		if ok || more {
			d.skipped += i
			d.buf = d.buf[i:]
			return
		}
	}
	// keep a tail that could be the start of a magic
	keep := min(len(d.buf), 3)
	d.skipped += len(d.buf) - keep
	d.buf = d.buf[len(d.buf)-keep:]
}

type candidateState uint8

const (
	candWait candidateState = iota
	candReject
	candAccept
)

// candidate judges the data at the start of the buffer while recovering.
func (d *Demuxer) candidate() candidateState {
	b := d.buf
	ok, more := plausibleStart(b)
	if more {
		return candWait
	}
	if !ok {
		return candReject
	}

	switch checkContent(b) {
	case contentNeedMore:
		return candWait
	case contentBad:
		return candReject
	}

	total := frameSize(b)
	if len(b) < total {
		// a big unconfirmed claim must not trap the real frames behind it
		if total > recoverBigFrame && startInside(b) {
			return candReject
		}
		if len(b) > recoverMaxWait {
			return candReject
		}
		return candWait
	}

	// confirmation: another frame, or the end of the data, follows
	if rest := b[total:]; len(rest) >= 4 {
		ok, more := plausibleStart(rest)
		if more {
			return candWait // too little of the next frame to tell
		}
		if !ok {
			return candReject
		}
	}
	return candAccept
}

// startInside reports whether a plausible frame start exists inside b past its
// first byte.
func startInside(b []byte) bool {
	for i := 1; i+4 <= len(b); i++ {
		if ok, _ := plausibleStart(b[i:]); ok {
			return true
		}
	}
	return false
}

type contentState uint8

const (
	contentGood contentState = iota
	contentNeedMore
	contentBad
)

// maxRecoverExtra bounds the video "additional header" accepted while
// recovering, the cameras send 4 or 8 bytes.
const maxRecoverExtra = 1024

// checkContent validates what a frame carries, beyond its magic and sizes.
// Only used while recovering: random bytes pass the header checks too often.
func checkContent(b []byte) contentState {
	le := binary.LittleEndian
	magic := le.Uint32(b)

	switch {
	case magic == magicInfoV1 || magic == magicInfoV2:
		return contentGood

	case magic >= magicIframe && magic <= magicPframeEnd:
		if len(b) < 24 {
			return contentNeedMore
		}
		payload, extra := le.Uint32(b[8:]), le.Uint32(b[12:])
		if payload == 0 || payload > maxMediaPayload || extra > maxRecoverExtra {
			return contentBad
		}
		start := 24 + int(extra)
		if len(b) < start+4 {
			return contentNeedMore
		}
		if !isAnnexB(b[start:]) {
			return contentBad
		}
		return contentGood

	case magic == magicAAC:
		payload := int(le.Uint16(b[4:]))
		if len(b) < 8+7 && payload >= 7 {
			return contentNeedMore
		}
		if payload < 7 {
			return contentBad
		}
		adts := b[8:]
		if adts[0] != 0xFF || adts[1]&0xF0 != 0xF0 {
			return contentBad
		}
		// 13-bit frame length, it covers the whole ADTS frame
		if n := int(adts[3]&3)<<11 | int(adts[4])<<3 | int(adts[5]>>5); n < 7 || n > payload {
			return contentBad
		}
		return contentGood

	case magic == magicADPCM:
		payload := int(le.Uint16(b[4:]))
		if payload < 8 {
			return contentBad
		}
		// the camera sends data/2 or 2, neolink's talk writes (data-4)/2
		half := int(le.Uint16(b[10:]))
		if half != 2 && half != (payload-4)/2 && half != (payload-8)/2 {
			return contentBad
		}
		return contentGood
	}
	return contentBad
}

func isAnnexB(b []byte) bool {
	return len(b) >= 4 && b[0] == 0 && b[1] == 0 && (b[2] == 1 || (b[2] == 0 && b[3] == 1))
}

// frameSize is the full size of a frame, padding included, from its header.
// The header must be complete and plausible.
func frameSize(b []byte) int {
	le := binary.LittleEndian
	magic := le.Uint32(b)

	switch {
	case magic == magicInfoV1 || magic == magicInfoV2:
		return 32
	case magic >= magicIframe && magic <= magicPframeEnd:
		payload, extra := int(le.Uint32(b[8:])), int(le.Uint32(b[12:]))
		return 24 + extra + payload + pad(payload)
	}
	payload := int(le.Uint16(b[4:]))
	return 8 + payload + pad(payload)
}

// plausibleStart reports whether b begins a frame. more is true when b is a
// valid prefix that is too short to decide.
func plausibleStart(b []byte) (ok, more bool) {
	if len(b) < 4 {
		return false, true
	}
	magic := binary.LittleEndian.Uint32(b)
	switch {
	case magic == magicInfoV1 || magic == magicInfoV2:
		if len(b) < 8 {
			return false, true
		}
		return binary.LittleEndian.Uint32(b[4:]) == 32, false
	case magic >= magicIframe && magic <= magicIframeEnd, magic >= magicPframe && magic <= magicPframeEnd:
		if len(b) < 8 {
			return false, true
		}
		c := string(b[4:8])
		return c == "H264" || c == "H265", false
	case magic == magicAAC:
		if len(b) < 8 {
			return false, true
		}
		return binary.LittleEndian.Uint16(b[4:]) == binary.LittleEndian.Uint16(b[6:]), false
	case magic == magicADPCM:
		if len(b) < 12 {
			return false, true
		}
		return binary.LittleEndian.Uint16(b[8:]) == adpcmDataMagic &&
			binary.LittleEndian.Uint16(b[4:]) == binary.LittleEndian.Uint16(b[6:]), false
	}
	return false, false
}

func pad(n int) int { return (mediaPad - n%mediaPad) % mediaPad }

// parseMedia decodes one frame from the start of b. It returns
// errMediaIncomplete if b is a valid prefix, any other error if it is not a
// frame at all.
func parseMedia(b []byte) (*Media, int, error) {
	ok, more := plausibleStart(b)
	if more {
		return nil, 0, errMediaIncomplete
	}
	if !ok {
		return nil, 0, errors.New("bcmedia: unknown magic")
	}
	magic := binary.LittleEndian.Uint32(b)
	le := binary.LittleEndian

	switch {
	case magic == magicInfoV1 || magic == magicInfoV2:
		const size = 32
		if len(b) < size {
			return nil, 0, errMediaIncomplete
		}
		m := &Media{Kind: KindInfoV1}
		if magic == magicInfoV2 {
			m.Kind = KindInfoV2
		}
		m.Info.Width = le.Uint32(b[8:])
		m.Info.Height = le.Uint32(b[12:])
		// b[16] unknown
		m.Info.FPS = b[17]
		copy(m.Info.Start[:], b[18:24])
		copy(m.Info.End[:], b[24:30])
		// b[30:32] unknown
		return m, size, nil

	case magic >= magicIframe && magic <= magicIframeEnd, magic >= magicPframe && magic <= magicPframeEnd:
		const hdr = 24 // magic, codec, payload size, extra header size, microseconds, unknown
		if len(b) < hdr {
			return nil, 0, errMediaIncomplete
		}
		payload, extra := le.Uint32(b[8:]), le.Uint32(b[12:])
		if payload > maxMediaPayload || extra > maxMediaPayload {
			return nil, 0, errors.New("bcmedia: frame too large")
		}
		total := hdr + int(extra) + int(payload) + pad(int(payload))
		if len(b) < total {
			return nil, 0, errMediaIncomplete
		}
		m := &Media{
			Kind:         KindPframe,
			Codec:        string(b[4:8]),
			Microseconds: le.Uint32(b[16:]),
		}
		if magic <= magicIframeEnd {
			m.Kind = KindIframe
			// extra header: POSIX time, anything after is skipped
			if extra >= 4 {
				m.Time, m.HasTime = le.Uint32(b[hdr:]), true
			}
		}
		start := hdr + int(extra)
		m.Data = append([]byte(nil), b[start:start+int(payload)]...)
		return m, total, nil

	case magic == magicAAC:
		const hdr = 8
		payload := int(le.Uint16(b[4:]))
		total := hdr + payload + pad(payload)
		if len(b) < total {
			return nil, 0, errMediaIncomplete
		}
		return &Media{Kind: KindAAC, Data: append([]byte(nil), b[hdr:hdr+payload]...)}, total, nil

	default: // magicADPCM
		// payload_size includes the 4-byte sub-header (data magic + half block)
		const hdr, sub = 8, 4
		payload := int(le.Uint16(b[4:]))
		if payload < sub {
			return nil, 0, errors.New("bcmedia: adpcm payload smaller than sub-header")
		}
		total := hdr + payload + pad(payload)
		if len(b) < total {
			return nil, 0, errMediaIncomplete
		}
		return &Media{Kind: KindADPCM, Data: append([]byte(nil), b[hdr+sub:hdr+payload]...)}, total, nil
	}
}

// Marshal serializes the frame, used to send talk audio. Padding follows the
// camera's own streams: the padded unit is the payload_size field, which for
// ADPCM includes the 4-byte sub-header.
func (m *Media) Marshal() ([]byte, error) {
	le := binary.LittleEndian
	switch m.Kind {
	case KindInfoV1, KindInfoV2:
		b := make([]byte, 32)
		magic := uint32(magicInfoV1)
		if m.Kind == KindInfoV2 {
			magic = magicInfoV2
		}
		le.PutUint32(b, magic)
		le.PutUint32(b[4:], 32)
		le.PutUint32(b[8:], m.Info.Width)
		le.PutUint32(b[12:], m.Info.Height)
		b[17] = m.Info.FPS
		copy(b[18:], m.Info.Start[:])
		copy(b[24:], m.Info.End[:])
		return b, nil

	case KindIframe, KindPframe:
		if m.Codec != "H264" && m.Codec != "H265" {
			return nil, fmt.Errorf("bcmedia: bad codec %q", m.Codec)
		}
		magic, extra := uint32(magicPframe), 0
		if m.Kind == KindIframe {
			magic = magicIframe
			if m.HasTime {
				extra = 8
			}
		}
		b := make([]byte, 24, 24+extra+len(m.Data)+mediaPad)
		le.PutUint32(b, magic)
		copy(b[4:], m.Codec)
		le.PutUint32(b[8:], uint32(len(m.Data)))
		le.PutUint32(b[12:], uint32(extra))
		le.PutUint32(b[16:], m.Microseconds)
		if extra > 0 {
			b = le.AppendUint32(b, m.Time)
			b = le.AppendUint32(b, 0)
		}
		b = append(b, m.Data...)
		return append(b, make([]byte, pad(len(m.Data)))...), nil

	case KindAAC:
		if len(m.Data) > 0xffff {
			return nil, errors.New("bcmedia: aac frame too large")
		}
		b := make([]byte, 8, 8+len(m.Data)+mediaPad)
		le.PutUint32(b, magicAAC)
		le.PutUint16(b[4:], uint16(len(m.Data)))
		le.PutUint16(b[6:], uint16(len(m.Data)))
		b = append(b, m.Data...)
		return append(b, make([]byte, pad(len(m.Data)))...), nil

	case KindADPCM:
		if len(m.Data) < 4 || len(m.Data)+4 > 0xffff {
			return nil, errors.New("bcmedia: bad adpcm block size")
		}
		payload := len(m.Data) + 4
		b := make([]byte, 12, 12+len(m.Data)+mediaPad)
		le.PutUint32(b, magicADPCM)
		le.PutUint16(b[4:], uint16(payload))
		le.PutUint16(b[6:], uint16(payload))
		le.PutUint16(b[8:], adpcmDataMagic)
		le.PutUint16(b[10:], uint16((len(m.Data)-4)/2)) // half the block without its header
		b = append(b, m.Data...)
		return append(b, make([]byte, pad(payload))...), nil
	}
	return nil, fmt.Errorf("bcmedia: cannot marshal %s", m.Kind)
}

// MarshalTalk serializes one ADPCM block (as made by ADPCMEncoder) as a
// BcMedia frame for a talk message (msg 202). It is not Media.Marshal: for
// the known-working talk path neolink pads by len(data) % 8, without the
// four sub-header bytes that the camera's own streams, and so the receive
// parser, include in the padded size.
func MarshalTalk(block []byte) ([]byte, error) {
	if len(block) < 4 || len(block) > MaxADPCMDataBytes+4 {
		return nil, errors.New("bcmedia: bad adpcm block size")
	}
	le := binary.LittleEndian
	payload := len(block) + 4
	b := make([]byte, 12, 12+len(block)+mediaPad)
	le.PutUint32(b, magicADPCM)
	le.PutUint16(b[4:], uint16(payload))
	le.PutUint16(b[6:], uint16(payload))
	le.PutUint16(b[8:], adpcmDataMagic)
	le.PutUint16(b[10:], uint16((len(block)-4)/2))
	b = append(b, block...)
	return append(b, make([]byte, pad(len(block)))...), nil
}

// maxClockJump: a camera clock step bigger than this is a reset, not time.
const maxClockJump = 10_000_000 // µs

// videoClock turns the camera's wrapping u32 microsecond counter into a
// continuous clock. The first frame is 0, deltas are taken with wrapping
// subtraction (a u32 wraps every ~71 min) and a delta over 10 s is taken as
// a camera clock reset: the clock is held and the reference re-synced.
type videoClock struct {
	started bool
	last    uint32
	acc     uint64 // µs
}

func (v *videoClock) next(us uint32) uint64 {
	if !v.started {
		v.started = true
	} else if d := us - v.last; d <= maxClockJump {
		v.acc += uint64(d)
	}
	v.last = us
	return v.acc
}

// maxAudioDrift is how far the audio clock may fall behind the video clock
// before it is moved up to it. Frames the transport gave up on (UDP gap skips)
// carry no duration, so over a long session audio would drift earlier. Audio
// ahead of video is left alone: video may just be paused or bursty, and the
// audio timeline must never go backwards.
const maxAudioDrift = 500_000 // µs

// audioClock paces audio by its own content duration. Audio has no camera
// timestamp, it is anchored to the video clock and then advances by the
// samples of every frame, delivered or not.
type audioClock struct {
	on   bool
	rate uint32
	pos  uint64
}

// next returns the position of a frame of the given samples and advances.
// videoUS is the video clock at that moment (videoOK: there is video), used
// when (re)anchoring. reanchored tells that the clock was put back on video.
func (a *audioClock) next(rate uint32, samples, videoUS uint64, videoOK bool) (pos uint64, reanchored bool) {
	switch {
	case !a.on || a.rate != rate:
		a.on, a.rate = true, rate
		a.pos = videoUS * uint64(rate) / 1_000_000
	case videoOK:
		if audioUS := a.pos * 1_000_000 / uint64(rate); videoUS > audioUS+maxAudioDrift {
			a.pos = videoUS * uint64(rate) / 1_000_000
			reanchored = true
		}
	}
	pos = a.pos
	a.pos += samples
	return pos, reanchored
}

var adtsRates = [13]uint32{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// adtsFrame is one ADTS frame inside an AAC payload.
type adtsFrame struct {
	header, size int // header bytes, whole frame bytes
	blocks       int // raw data blocks, 1024 samples each
}

type adtsInfo struct {
	rate    uint32
	format  uint32 // profile, rate index and channels, to spot a format change
	samples uint64 // in the whole payload
	frames  []adtsFrame
}

// parseADTS splits an AAC payload into ADTS frames. It fails if the payload
// does not start with a valid frame; trailing garbage is ignored.
func parseADTS(b []byte) (info adtsInfo, ok bool) {
	for len(b) >= 7 && b[0] == 0xFF && b[1]&0xF0 == 0xF0 {
		idx := int(b[2]>>2) & 0xF
		if idx >= len(adtsRates) {
			break
		}
		header := 7
		if b[1]&1 == 0 { // protection absent bit clear: 2 bytes CRC
			header = 9
		}
		size := int(b[3]&3)<<11 | int(b[4])<<3 | int(b[5]>>5)
		if size < header || size > len(b) {
			break
		}
		blocks := int(b[6]&3) + 1

		if len(info.frames) == 0 {
			info.rate = adtsRates[idx]
			info.format = uint32(b[2]>>6)<<16 | uint32(idx)<<8 | uint32(b[2]&1)<<2 | uint32(b[3]>>6)
		}
		info.frames = append(info.frames, adtsFrame{header: header, size: size, blocks: blocks})
		info.samples += uint64(blocks) * 1024
		b = b[size:]
	}
	return info, len(info.frames) > 0
}
