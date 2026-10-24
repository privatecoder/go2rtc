package bc

// resampler converts PCM between sample rates with linear interpolation. It
// is stateful: the fractional position and the last sample carry over from
// one packet to the next, so how the audio is cut into packets changes
// neither the number of samples nor their values. Positions are exact
// integers, in units of 1/dst of a source sample.
type resampler struct {
	src, dst int64
	next     int64 // position of the next output, relative to the start of the next input
	prev     int16 // last input sample of the previous packet
}

func newResampler(srcRate, dstRate uint32) *resampler {
	return &resampler{src: int64(srcRate), dst: int64(dstRate)}
}

// process returns exactly the samples that the input completes.
func (r *resampler) process(in []int16) []int16 {
	n := int64(len(in))
	if n == 0 {
		return nil
	}
	if r.src == r.dst {
		return append([]int16(nil), in...)
	}

	out := make([]int16, 0, n*r.dst/r.src+2)
	// an output needs the input sample after its position
	for limit := (n - 1) * r.dst; r.next < limit; r.next += r.src {
		idx := r.next / r.dst
		if r.next < 0 {
			idx = -1 // between the previous packet's last sample and our first
		}
		rem := r.next - idx*r.dst

		a := r.prev
		if idx >= 0 {
			a = in[idx]
		}
		b := in[idx+1]
		out = append(out, int16(int64(a)+(int64(b)-int64(a))*rem/r.dst))
	}

	r.next -= n * r.dst
	r.prev = in[n-1]
	return out
}
