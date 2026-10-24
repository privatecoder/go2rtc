package bc

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Reolink uses two IMA ADPCM layouts that must not be mixed up.
//
// Received (camera to us, BcMedia): a 4-byte header (int16 LE predictor,
// uint8 step index, 0) is the decoder state carried from the previous block,
// it is not an output sample. The 4-bit samples follow, HIGH nibble first, so
// n data bytes decode to exactly 2n samples (the predictor equals the last
// sample of the previous block).
//
// Sent (talk, us to camera): what GStreamer `adpcmenc layout=dvi` makes, the
// IMA WAV block: the same header, but its predictor IS the first sample, then
// the next samples with the LOW nibble first, so n data bytes hold 1+2n samples.

var imaStepTable = [89]int32{
	7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45,
	50, 55, 60, 66, 73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230,
	253, 279, 307, 337, 371, 408, 449, 494, 544, 598, 658, 724, 796, 876, 963,
	1060, 1166, 1282, 1411, 1552, 1707, 1878, 2066, 2272, 2499, 2749, 3024, 3327,
	3660, 4026, 4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493, 10442,
	11487, 12635, 13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794,
	32767,
}

var imaIndexTable = [8]int{-1, -1, -1, -1, 2, 4, 6, 8}

const adpcmHeaderLen = 4

// ADPCMSamples is the number of PCM samples a talk block with dataBytes data
// bytes (header excluded) holds, and what EncodeBlock consumes. A received
// block decodes to 2*dataBytes samples, see DecodeADPCM.
func ADPCMSamples(dataBytes int) int { return 1 + 2*dataBytes }

type imaState struct {
	pred  int32
	index int
}

// step decodes one nibble, updating the state.
func (s *imaState) step(nib byte) int16 {
	step := imaStepTable[s.index]
	diff := step >> 3
	if nib&4 != 0 {
		diff += step
	}
	if nib&2 != 0 {
		diff += step >> 1
	}
	if nib&1 != 0 {
		diff += step >> 2
	}
	if nib&8 != 0 {
		s.pred -= diff
	} else {
		s.pred += diff
	}
	s.pred = clamp16(s.pred)
	s.index = min(max(s.index+imaIndexTable[nib&7], 0), 88)
	return int16(s.pred)
}

// encodeNibble finds the nibble for sample v and advances the state exactly
// as the decoder will.
func (s *imaState) encodeNibble(v int16) byte {
	step := imaStepTable[s.index]
	diff := int32(v) - s.pred
	var nib byte
	if diff < 0 {
		nib = 8
		diff = -diff
	}
	if diff >= step {
		nib |= 4
		diff -= step
	}
	if diff >= step>>1 {
		nib |= 2
		diff -= step >> 1
	}
	if diff >= step>>2 {
		nib |= 1
	}
	s.step(nib)
	return nib
}

func clamp16(v int32) int32 {
	return min(max(v, -32768), 32767)
}

// DecodeADPCM decodes one received block (4-byte header + data, high nibble
// first) to 2*(len(block)-4) samples. The header only initializes the state.
func DecodeADPCM(block []byte) ([]int16, error) {
	if len(block) < adpcmHeaderLen {
		return nil, errors.New("adpcm: block shorter than header")
	}
	s := imaState{
		pred:  int32(int16(binary.LittleEndian.Uint16(block))),
		index: int(block[2]),
	}
	if s.index > 88 {
		return nil, errors.New("adpcm: bad step index")
	}
	pcm := make([]int16, 0, 2*(len(block)-adpcmHeaderLen))
	for _, b := range block[adpcmHeaderLen:] {
		pcm = append(pcm, s.step(b>>4), s.step(b&0x0f))
	}
	return pcm, nil
}

// MaxADPCMDataBytes is the largest block a talk frame can carry: the BcMedia
// size field is 16 bits and holds the data plus an 8-byte envelope.
const MaxADPCMDataBytes = 0xffff - 8

// ADPCMEncoder encodes PCM to talk blocks (IMA WAV / GStreamer DVI layout).
// The step index carries over from block to block, the predictor restarts at
// each block's first sample.
type ADPCMEncoder struct {
	index int
}

// EncodeBlock encodes pcm into one block of dataBytes data bytes plus header,
// low nibble first. It consumes ADPCMSamples(dataBytes) samples; a short pcm
// is padded by repeating its last sample (silence if empty).
func (e *ADPCMEncoder) EncodeBlock(pcm []int16, dataBytes int) ([]byte, error) {
	if dataBytes <= 0 || dataBytes > MaxADPCMDataBytes {
		return nil, fmt.Errorf("adpcm: block data size %d out of range 1..%d", dataBytes, MaxADPCMDataBytes)
	}

	want := ADPCMSamples(dataBytes)
	if len(pcm) < want {
		p := make([]int16, want)
		n := copy(p, pcm)
		if n > 0 {
			for i := n; i < want; i++ {
				p[i] = p[n-1]
			}
		}
		pcm = p
	}

	s := imaState{pred: int32(pcm[0]), index: e.index}
	out := make([]byte, adpcmHeaderLen+dataBytes)
	binary.LittleEndian.PutUint16(out, uint16(pcm[0]))
	out[2] = byte(s.index)
	for i := 0; i < dataBytes; i++ {
		lo := s.encodeNibble(pcm[1+2*i])
		hi := s.encodeNibble(pcm[2+2*i])
		out[adpcmHeaderLen+i] = hi<<4 | lo
	}
	e.index = s.index
	return out, nil
}
