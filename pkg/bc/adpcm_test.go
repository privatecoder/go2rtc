package bc

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func adpcmSample(t testing.TB) (blocks [][]byte) {
	t.Helper()
	var d Demuxer
	d.Push(readTestdata(t, "adpcm_0.raw"))
	for _, m := range drain(&d) {
		blocks = append(blocks, m.Data)
	}
	require.Len(t, blocks, 2)
	return
}

// decodeIMALowFirst decodes the talk layout (IMA WAV, what GStreamer
// `adpcmenc layout=dvi` writes): the header predictor is the first sample,
// the low nibble comes first. 1+2n samples for n data bytes.
func decodeIMALowFirst(block []byte) []int16 {
	s := imaState{pred: int32(int16(binary.LittleEndian.Uint16(block))), index: int(block[2])}
	pcm := []int16{int16(s.pred)}
	for _, b := range block[adpcmHeaderLen:] {
		pcm = append(pcm, s.step(b&0x0f), s.step(b>>4))
	}
	return pcm
}

func TestDecodeADPCMReal(t *testing.T) {
	blocks := adpcmSample(t)

	// Expected values come from an independent reference decoder (python)
	// run over the camera's own bytes: high nibble first, the header is only
	// the state, exactly 2 samples per data byte.
	pcm0, err := DecodeADPCM(blocks[0])
	require.NoError(t, err)
	require.Len(t, pcm0, 480)
	require.Equal(t, []int16{55, 45, 48, 39, 21, 23, 57, 72}, pcm0[:8])
	require.Equal(t, []int16{-95, -68, -114, -96}, pcm0[476:])
	require.Equal(t, 40579, sum(pcm0))

	pcm1, err := DecodeADPCM(blocks[1])
	require.NoError(t, err)
	require.Len(t, pcm1, 480)
	require.Equal(t, []int16{-46, -92, -86, -81, -96, -65, -94, -76}, pcm1[:8])
	require.Equal(t, []int16{30, -7, 26, 32}, pcm1[476:])
	require.Equal(t, 35451, sum(pcm1))

	// the next header carries the state at the end of this block
	require.Equal(t, int16(binary.LittleEndian.Uint16(blocks[1])), pcm0[479])

	// cumulative count: no sample is added at block boundaries
	require.Len(t, append(pcm0, pcm1...), 2*2*240)
}

func sum(p []int16) (n int) {
	for _, v := range p {
		n += int(v)
	}
	return
}

func TestDecodeADPCMErrors(t *testing.T) {
	_, err := DecodeADPCM([]byte{1, 2, 3})
	require.Error(t, err)
	_, err = DecodeADPCM([]byte{0, 0, 89, 0})
	require.Error(t, err)

	pcm, err := DecodeADPCM([]byte{5, 0, 0, 0})
	require.NoError(t, err)
	require.Empty(t, pcm)

	pcm, err = DecodeADPCM([]byte{5, 0, 0, 0, 0x00})
	require.NoError(t, err)
	require.Len(t, pcm, 2)
}

// The talk vectors were made with an independent python implementation of the
// IMA step/quantize rules in GStreamer's adpcmenc.c and checked by wrapping
// the blocks in a WAV (format 0x11, adpcm_ima_wav) and decoding them with
// ffmpeg, which gave the same samples. The first compressed sample is in the
// LOW nibble: pcm 10 and 20 from index 0 give 0x36, not 0x63.
func TestEncodeBlockVector(t *testing.T) {
	ramp := make([]int16, 33)
	for i := range ramp {
		ramp[i] = int16(i * 10)
	}
	var enc ADPCMEncoder
	b, err := enc.EncodeBlock(ramp, 16)
	require.NoError(t, err)
	require.Equal(t, "0000000036332533343334533243334333253334", hex.EncodeToString(b))
	require.Equal(t, 5, enc.index)
	require.Equal(t, []int16{
		0, 10, 20, 30, 38, 51, 59, 69, 79, 91, 101, 111, 119, 130, 140, 148, 161,
		169, 179, 189, 201, 211, 221, 229, 240, 250, 258, 271, 279, 289, 299, 311, 321,
	}, decodeIMALowFirst(b))

	// signed samples and a start index other than 0
	sine := []int16{
		0, 5153, 7883, 6905, 2679, -2806, -6972, -7859, -5050, 134, 5255, 7905, 6836, 2552,
		-2931, -7037, -7833, -4945, 268, 5356, 7924, 6765, 2424, -3056, -7100, -7805, -4838,
		403, 5455, 7942, 6693, 2296, -3180,
	}
	enc = ADPCMEncoder{index: 20}
	b, err = enc.EncodeBlock(sine, 16)
	require.NoError(t, err)
	require.Equal(t, "000014007777ff2a4481cb8b5213c8bb284591da", hex.EncodeToString(b))
	require.Equal(t, 72, enc.index)
	require.Equal(t, []int16{
		0, 93, 292, 722, 1647, -340, -4600, -7643, -4876, -347, 5132, 7341, 6672, 2412,
		-2569, -7256, -7864, -5097, 438, 5594, 7602, 6994, 2013, -2674, -6934, -7487, -4971,
		61, 6088, 8519, 6310, 2962, -3734,
	}, decodeIMALowFirst(b))
}

func TestEncodeBlockRoundTripSine(t *testing.T) {
	const rate, freq, dataBytes = 16000, 440.0, 512
	per := ADPCMSamples(dataBytes)
	src := make([]int16, per*4)
	for i := range src {
		src[i] = int16(12000 * math.Sin(2*math.Pi*freq*float64(i)/rate))
	}

	var enc ADPCMEncoder
	var out []int16
	var prevIndex byte
	for off := 0; off+per <= len(src); off += per {
		block, err := enc.EncodeBlock(src[off:], dataBytes)
		require.NoError(t, err)
		require.Len(t, block, 4+dataBytes)
		if off > 0 {
			require.Equal(t, prevIndex, block[2], "step index carries over")
		}
		prevIndex = byte(enc.index)
		pcm := decodeIMALowFirst(block)
		require.Len(t, pcm, per)
		out = append(out, pcm...)
	}
	require.Len(t, out, len(src))

	var sig, noise float64
	for i := range out {
		d := float64(src[i]) - float64(out[i])
		sig += float64(src[i]) * float64(src[i])
		noise += d * d
	}
	snr := 10 * math.Log10(sig/noise)
	require.Greater(t, snr, 25.0, "SNR %.1f dB", snr)
}

func TestEncodeBlockShortAndExtremes(t *testing.T) {
	var enc ADPCMEncoder

	// short and empty input is padded, never panics
	b, err := enc.EncodeBlock([]int16{100, 200, 300}, 16)
	require.NoError(t, err)
	require.Len(t, b, 20)
	b, err = enc.EncodeBlock(nil, 16)
	require.NoError(t, err)
	require.Len(t, b, 20)
	require.Len(t, decodeIMALowFirst(b), 33)

	// full scale square wave stays inside int16
	sq := make([]int16, ADPCMSamples(64))
	for i := range sq {
		sq[i] = math.MaxInt16
		if i%2 == 1 {
			sq[i] = math.MinInt16
		}
	}
	b, err = enc.EncodeBlock(sq, 64)
	require.NoError(t, err)
	require.Equal(t, int16(math.MaxInt16), decodeIMALowFirst(b)[0])
}

func TestEncodeBlockSizeLimits(t *testing.T) {
	var enc ADPCMEncoder

	for _, n := range []int{-1, 0, math.MinInt, MaxADPCMDataBytes + 1, math.MaxInt32, math.MaxInt - 1, math.MaxInt} {
		b, err := enc.EncodeBlock(nil, n)
		require.Error(t, err, "dataBytes %d", n)
		require.Nil(t, b)
	}

	b, err := enc.EncodeBlock(nil, 1)
	require.NoError(t, err)
	require.Len(t, b, 5)

	b, err = enc.EncodeBlock(nil, MaxADPCMDataBytes)
	require.NoError(t, err)
	require.Len(t, b, 4+MaxADPCMDataBytes)

	// the largest block still fits the 16-bit BcMedia size fields
	w, err := MarshalTalk(b)
	require.NoError(t, err)
	require.Equal(t, uint16(0xffff), binary.LittleEndian.Uint16(w[4:]))
}

// parseTalk reads a frame made by MarshalTalk, the receive Demuxer cannot
// because the talk padding differs.
func parseTalk(t *testing.T, w []byte) (block, rest []byte) {
	t.Helper()
	require.Equal(t, uint32(magicADPCM), binary.LittleEndian.Uint32(w))
	payload := int(binary.LittleEndian.Uint16(w[4:]))
	require.Equal(t, payload, int(binary.LittleEndian.Uint16(w[6:])))
	require.Equal(t, uint16(adpcmDataMagic), binary.LittleEndian.Uint16(w[8:]))
	n := payload - 4
	require.Equal(t, n-4, int(binary.LittleEndian.Uint16(w[10:]))*2, "half block field is (data-4)/2 like neolink")
	block = w[12 : 12+n]
	return block, w[12+n+(8-n%8)%8:]
}

func TestMarshalTalk(t *testing.T) {
	const dataBytes = 512 // lengthPerEncoder 1024
	var enc ADPCMEncoder
	pcm := make([]int16, 2*ADPCMSamples(dataBytes))
	for i := range pcm {
		pcm[i] = int16(i % 2000)
	}

	var wire []byte
	var blocks [][]byte
	for off := 0; off < len(pcm); off += ADPCMSamples(dataBytes) {
		blk, err := enc.EncodeBlock(pcm[off:], dataBytes)
		require.NoError(t, err)
		blocks = append(blocks, blk)
		w, err := MarshalTalk(blk)
		require.NoError(t, err)
		// 12 byte envelope + 516 byte block + 4 bytes padding to 8 by len(data)
		require.Len(t, w, 12+516+4)
		wire = append(wire, w...)
	}

	rest := wire
	for _, want := range blocks {
		var got []byte
		got, rest = parseTalk(t, rest)
		require.Equal(t, want, got)
	}
	require.Empty(t, rest)

	// the camera's own streams pad by payload_size (data + 4), so the same
	// block is 4 bytes shorter in receive format
	w, err := (&Media{Kind: KindADPCM, Data: blocks[0]}).Marshal()
	require.NoError(t, err)
	require.Len(t, w, 12+516)

	_, err = MarshalTalk([]byte{1, 2, 3})
	require.Error(t, err)
	_, err = MarshalTalk(make([]byte, MaxADPCMDataBytes+5))
	require.Error(t, err)
}
