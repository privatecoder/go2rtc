package bc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func concat(t testing.TB, prefix string, n int) []byte {
	t.Helper()
	var out []byte
	for i := 0; i < n; i++ {
		out = append(out, readTestdata(t, fmt.Sprintf("%s_%d.raw", prefix, i))...)
	}
	return out
}

func swan(t testing.TB) []byte {
	t.Helper()
	var out []byte
	for i := 0; i < 10; i++ {
		out = append(out, readTestdata(t, fmt.Sprintf("video_stream_swan_%02d.raw", i))...)
	}
	return out
}

func drain(d *Demuxer) (out []*Media) {
	for m := d.Next(); m != nil; m = d.Next() {
		out = append(out, m)
	}
	return
}

func TestInfoV1(t *testing.T) {
	var d Demuxer
	d.Push(readTestdata(t, "info_v1.raw"))
	m := d.Next()
	require.NotNil(t, m)
	require.Equal(t, KindInfoV1, m.Kind)
	require.Equal(t, StreamInfo{
		Width: 2560, Height: 1440, FPS: 30,
		Start: [6]uint8{121, 8, 4, 23, 23, 52},
		End:   [6]uint8{121, 8, 4, 23, 23, 52},
	}, m.Info)
	require.Zero(t, d.Buffered())
	require.Nil(t, d.Next())
}

func TestIframe(t *testing.T) {
	var d Demuxer
	d.Push(concat(t, "iframe", 5))
	m := d.Next()
	require.NotNil(t, m)
	require.Equal(t, KindIframe, m.Kind)
	require.Equal(t, "H264", m.Codec)
	require.Equal(t, uint32(3557705112), m.Microseconds)
	require.True(t, m.HasTime)
	require.Equal(t, uint32(1628085232), m.Time)
	require.Len(t, m.Data, 192881)
	// Annex-B start code
	require.Equal(t, []byte{0, 0, 0, 1}, m.Data[:4])
	require.Zero(t, d.Buffered())
}

func TestPframe(t *testing.T) {
	var d Demuxer
	d.Push(concat(t, "pframe", 2))
	m := d.Next()
	require.NotNil(t, m)
	require.Equal(t, KindPframe, m.Kind)
	require.Equal(t, "H264", m.Codec)
	require.Equal(t, uint32(3557767112), m.Microseconds)
	require.False(t, m.HasTime)
	require.Len(t, m.Data, 45108)
	require.Zero(t, d.Buffered())
}

func TestArgus2Extended(t *testing.T) {
	// extended headers: extra bytes after the time are skipped
	var d Demuxer
	d.Push(concat(t, "argus2_iframe", 5))
	d.Push(concat(t, "argus2_pframe", 18))
	frames := drain(&d)
	require.NotEmpty(t, frames)
	require.Zero(t, d.Buffered())
	require.Zero(t, d.Skipped())

	var i, p int
	for _, m := range frames {
		switch m.Kind {
		case KindIframe:
			i++
			require.True(t, m.HasTime)
		case KindPframe:
			p++
		case KindAAC, KindADPCM:
			require.NotEmpty(t, m.Data)
			continue
		default:
			t.Fatalf("unexpected %s", m.Kind)
		}
		require.Contains(t, []string{"H264", "H265"}, m.Codec)
		require.NotEmpty(t, m.Data)
	}
	require.NotZero(t, i)
	require.NotZero(t, p)
}

func TestADPCMFrames(t *testing.T) {
	var d Demuxer
	d.Push(readTestdata(t, "adpcm_0.raw"))
	frames := drain(&d)
	require.Len(t, frames, 2)
	for _, m := range frames {
		require.Equal(t, KindADPCM, m.Kind)
		require.Len(t, m.Data, 244)
	}
	require.Zero(t, d.Buffered())
}

func TestSwanStream(t *testing.T) {
	stream := swan(t)

	var d Demuxer
	d.Push(stream)
	want := drain(&d)
	require.Zero(t, d.Buffered())
	require.Zero(t, d.Skipped())

	kinds := map[MediaKind]int{}
	for _, m := range want {
		kinds[m.Kind]++
	}
	require.NotZero(t, kinds[KindIframe])
	require.NotZero(t, kinds[KindPframe])
	require.NotZero(t, kinds[KindADPCM])

	// any chunking gives the same frames
	for _, size := range []int{1, 3, 7, 8, 100, 1021, 4096, 40000, len(stream)} {
		var d Demuxer
		var got []*Media
		for off := 0; off < len(stream); off += size {
			end := min(off+size, len(stream))
			d.Push(stream[off:end])
			got = append(got, drain(&d)...)
		}
		require.Equal(t, want, got, "chunk size %d", size)
		require.Zero(t, d.Buffered(), "chunk size %d", size)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	stream := swan(t)

	var d Demuxer
	d.Push(stream)
	frames := drain(&d)

	// Video frames carry unknown header words we do not keep, so compare
	// them semantically; audio frames must be reproduced byte for byte.
	var out, audioIn, audioOut []byte
	off := 0
	for _, m := range frames {
		b, err := m.Marshal()
		require.NoError(t, err)
		out = append(out, b...)
		if m.Kind == KindADPCM || m.Kind == KindAAC {
			in := bytes.Clone(stream[off : off+len(b)])
			if m.Kind == KindADPCM {
				// the camera sends len(data)/2 in the "half block" field, neolink
				// writes (len(data)-4)/2 and the camera ignores it on talk
				in[10], in[11] = b[10], b[11]
			}
			audioOut = append(audioOut, b...)
			audioIn = append(audioIn, in...)
		}
		off += len(b)
	}
	require.Equal(t, len(stream), len(out))
	require.NotEmpty(t, audioIn)
	require.Equal(t, audioIn, audioOut)

	var d2 Demuxer
	d2.Push(out)
	require.Equal(t, frames, drain(&d2))
}

func TestMarshalFrames(t *testing.T) {
	frames := []*Media{
		{Kind: KindInfoV2, Info: StreamInfo{Width: 640, Height: 360, FPS: 10, Start: [6]uint8{1, 2, 3, 4, 5, 6}}},
		{Kind: KindIframe, Codec: "H265", Microseconds: 7, Time: 99, HasTime: true, Data: []byte{1, 2, 3}},
		{Kind: KindIframe, Codec: "H264", Microseconds: 8, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{Kind: KindPframe, Codec: "H264", Microseconds: 9, Data: []byte{9}},
		{Kind: KindAAC, Data: []byte{0xff, 0xf1, 1, 2, 3}},
		{Kind: KindADPCM, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{Kind: KindADPCM, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	var d Demuxer
	for _, m := range frames {
		b, err := m.Marshal()
		require.NoError(t, err)
		require.Zero(t, len(b)%8, "%s not padded to 8", m.Kind)
		d.Push(b)
	}
	require.Equal(t, frames, drain(&d))
	require.Zero(t, d.Buffered())

	_, err := (&Media{Kind: KindIframe, Codec: "VP9"}).Marshal()
	require.Error(t, err)
	_, err = (&Media{Kind: KindADPCM, Data: []byte{1}}).Marshal()
	require.Error(t, err)
}

func TestDemuxerResync(t *testing.T) {
	stream := swan(t)
	var d Demuxer
	d.Push(stream)
	want := drain(&d)

	// split the stream at frame boundaries and put junk in between, junk
	// containing partial magics and a magic with a bad sanity field
	var parts [][]byte
	for _, m := range want {
		b, err := m.Marshal()
		require.NoError(t, err)
		parts = append(parts, b)
	}
	junk := []byte{0xde, 0xad, 'd', 'c', '0', '0', 0xff, 0xff, 0x30, 0x30, 0x31, 0x30, 1, 2, 3, 'w', 'b', '5', '0'}
	var dirty []byte
	for i, b := range parts {
		if i%3 == 1 {
			dirty = append(dirty, junk...)
		}
		dirty = append(dirty, b...)
	}

	for _, size := range []int{1, 13, len(dirty)} {
		var d Demuxer
		var got []*Media
		for off := 0; off < len(dirty); off += size {
			d.Push(dirty[off:min(off+size, len(dirty))])
			got = append(got, drain(&d)...)
		}
		require.Equal(t, want, got, "chunk size %d", size)
		require.NotZero(t, d.Skipped())
	}
}

func TestDemuxerRejects(t *testing.T) {
	le := func(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }

	// oversize iframe: dropped, not waited for
	var d Demuxer
	d.Push(le(magicIframe))
	d.Push([]byte("H264"))
	d.Push(le(maxMediaPayload + 1))
	d.Push(le(0))
	d.Push(le(0))
	d.Push(le(0))
	require.Nil(t, d.Next())
	require.Less(t, d.Buffered(), 4)

	// adpcm payload smaller than its sub-header
	d = Demuxer{}
	d.Push(le(magicADPCM))
	d.Push([]byte{2, 0, 2, 0, 0, 1, 0, 0})
	require.Nil(t, d.Next())
	require.Less(t, d.Buffered(), 4)
}

func TestMediaKindString(t *testing.T) {
	require.Equal(t, "Iframe", KindIframe.String())
	require.Equal(t, "Unknown", MediaKind(0).String())
}

// The recovery tests below are the reviewer's reproductions: after garbage a
// four byte magic inside damaged data must not swallow genuine frames.

func genuineP(t testing.TB, us uint32) (*Media, []byte) {
	t.Helper()
	m := &Media{Kind: KindPframe, Codec: "H264", Microseconds: us, Data: []byte{0, 0, 0, 1, 0x41, 0xaa, 0xbb, 0xcc}}
	b, err := m.Marshal()
	require.NoError(t, err)
	require.Len(t, b, 32)
	return m, b
}

func le32(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }

var junk = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

func TestRecoverFalseAAC(t *testing.T) {
	want, genuine := genuineP(t, 1000)

	// an AAC header whose 32 byte payload is exactly the genuine P-frame
	stream := append([]byte(nil), junk...)
	stream = append(stream, 0x30, 0x35, 0x77, 0x62, 0x20, 0x00, 0x20, 0x00)
	stream = append(stream, genuine...)

	var d Demuxer
	d.Push(stream)
	m := d.Next()
	require.NotNil(t, m, "genuine frame swallowed by a false AAC frame")
	require.Equal(t, want, m)
	require.Nil(t, d.Next())
	require.Zero(t, d.Buffered())

	// the same when the bytes arrive one by one
	d = Demuxer{}
	var got []*Media
	for _, b := range stream {
		d.Push([]byte{b})
		got = append(got, drain(&d)...)
	}
	require.Equal(t, []*Media{want}, got)
}

func TestRecoverFalseAACWithSyncword(t *testing.T) {
	want, genuine := genuineP(t, 7)

	// valid sizes and syncword but the ADTS frame length claims more than the payload
	adts := []byte{0xff, 0xf1, 0x50, 0x80, 0x10, 0x00, 0xfc}
	bad := append([]byte{0x30, 0x35, 0x77, 0x62, 0x20, 0x00, 0x20, 0x00}, adts...)
	bad = append(bad, make([]byte, 25)...)

	var d Demuxer
	d.Push(append(append(append([]byte(nil), junk...), bad...), genuine...))
	require.Equal(t, want, d.Next())
}

func TestRecoverFalseHugeVideo(t *testing.T) {
	want, genuine := genuineP(t, 42)

	// a false P-frame claiming the 16 MiB maximum, genuine frame right behind it
	for name, start := range map[string][]byte{
		"no start code": {1, 2, 3, 4},
		"start code":    {0, 0, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			stream := append([]byte(nil), junk...)
			stream = append(stream, le32(magicPframe)...)
			stream = append(stream, "H264"...)
			stream = append(stream, le32(maxMediaPayload)...)
			stream = append(stream, le32(0)...)
			stream = append(stream, le32(0)...)
			stream = append(stream, le32(0)...)
			stream = append(stream, start...)
			stream = append(stream, genuine...)

			var d Demuxer
			d.Push(stream)
			m := d.Next()
			require.NotNil(t, m, "genuine frame trapped behind a false 16 MiB frame")
			require.Equal(t, want, m)
		})
	}
}

func TestRecoverFalseADPCM(t *testing.T) {
	want, genuine := genuineP(t, 9)

	// right magic and equal sizes, but the half-block field fits no layout
	bad := append(le32(magicADPCM), 0x14, 0x00, 0x14, 0x00, 0x00, 0x01, 0x63, 0x00)
	bad = append(bad, make([]byte, 16)...)

	var d Demuxer
	d.Push(append(append(append([]byte(nil), junk...), bad...), genuine...))
	require.Equal(t, want, d.Next())
}

func TestRecoverNeedsConfirmation(t *testing.T) {
	// a well formed frame followed by garbage was most likely found inside a
	// damaged payload and is not handed out; a following frame confirms it
	want, genuine := genuineP(t, 1)
	_, genuine2 := genuineP(t, 2)

	var d Demuxer
	d.Push(append(append([]byte(nil), junk...), genuine...))
	d.Push(junk)
	require.Nil(t, d.Next())

	d = Demuxer{}
	d.Push(append(append(append([]byte(nil), junk...), genuine...), genuine2...))
	require.Equal(t, want, d.Next())

	// at the very end of the data there is nothing to contradict it
	d = Demuxer{}
	d.Push(append(append([]byte(nil), junk...), genuine...))
	require.Equal(t, want, d.Next())
}

func TestRecoverBoundsWait(t *testing.T) {
	// a plausible false frame (start code, 3 MiB) with no further frame start
	// anywhere must not make the demuxer buffer forever
	stream := append([]byte(nil), junk...)
	stream = append(stream, le32(magicPframe)...)
	stream = append(stream, "H265"...)
	stream = append(stream, le32(3*1024*1024)...)
	stream = append(stream, le32(0)...)
	stream = append(stream, le32(0)...)
	stream = append(stream, le32(0)...)
	stream = append(stream, 0, 0, 1, 0x02)

	var d Demuxer
	d.Push(stream)
	require.Nil(t, d.Next())
	require.Greater(t, d.Buffered(), 0)

	d.Push(make([]byte, recoverMaxWait))
	require.Nil(t, d.Next())
	require.Less(t, d.Buffered(), 8, "unconfirmed candidate still holds the buffer")
}

func TestLargeFramesStayLengthBased(t *testing.T) {
	// outside recovery nothing but the sizes matters, a 3 MiB keyframe without
	// start code is delivered once complete
	big := make([]byte, 3*1024*1024+3)
	m := &Media{Kind: KindIframe, Codec: "H265", Microseconds: 5, Data: big}
	b, err := m.Marshal()
	require.NoError(t, err)

	var d Demuxer
	for off := 0; off < len(b); off += 100_000 {
		d.Push(b[off:min(off+100_000, len(b))])
		if off+100_000 < len(b) {
			require.Nil(t, d.Next())
		}
	}
	got := d.Next()
	require.NotNil(t, got)
	require.Len(t, got.Data, len(big))
	require.Zero(t, d.Skipped())
}
