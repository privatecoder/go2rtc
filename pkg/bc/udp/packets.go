// Package udp is the Reolink P2P transport: UDP packets, the discovery
// handshake that turns a camera UID into a path (LAN, hole-punched or relayed)
// and a reliable byte stream on top of it for the Baichuan protocol.
package udp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	MagicDiscovery = 0x2a87cf3a // negotiation, carries a P2P XML message
	MagicData      = 0x2a87cf10 // a fragment of the Baichuan byte stream
	MagicAck       = 0x2a87cf20 // acknowledges Data and reports the receive rate

	// NoAck is group_id and packet_id of an ACK that acknowledges nothing yet.
	NoAck = 0xffffffff
)

var (
	ErrShort    = errors.New("udp: short packet")
	ErrMagic    = errors.New("udp: unknown magic")
	ErrChecksum = errors.New("udp: discovery checksum mismatch")
	ErrFormat   = errors.New("udp: malformed packet")
)

// Packet is a Discovery, Ack or Data.
type Packet interface {
	// Marshal encodes the datagram.
	Marshal() ([]byte, error)
}

// Discovery carries one P2P XML message. TID is a per-message id: replies echo
// it, and it keys the XML encryption.
type Discovery struct {
	TID uint32
	XML Message
}

// Ack acknowledges received Data. ConnID is the camera's id (did) when sent.
type Ack struct {
	ConnID  int32
	GroupID uint32 // 0, or NoAck
	// PacketID is the last Data packet received in order, NoAck if none yet.
	PacketID uint32
	// RecvBytesPerSec is the receiver's measured throughput, the bandwidth
	// estimate of the camera's rate controller. Not a latency.
	RecvBytesPerSec uint32
	// Payload is the selective ACK: one byte per packet after PacketID, 1 if
	// it was received and 0 if it is still missing.
	Payload []byte
}

// EmptyAck acknowledges nothing, sent before the first packet is in.
func EmptyAck(connID int32) *Ack {
	return &Ack{ConnID: connID, GroupID: NoAck, PacketID: NoAck}
}

// Data is one fragment of the byte stream. ConnID is the camera's id (did)
// when sent and ours (cid) when received.
type Data struct {
	ConnID   int32
	PacketID uint32
	Payload  []byte
}

// The XML of a Discovery packet is encrypted with a key derived from its tid
// and protected by a CRC.
var xmlKey = [8]uint32{
	0x1f2d3c4b, 0x5a6c7f8d, 0x38172e4b, 0x8271635a, 0x863f1a2b, 0xa5c6f7d8, 0x8371e1b4, 0x17f2d3a5,
}

// xmlCrypt encrypts and decrypts: XOR with a 32 byte key stream, each key word
// offset by the tid.
func xmlCrypt(tid uint32, b []byte) []byte {
	var key [32]byte
	for i, w := range xmlKey {
		binary.LittleEndian.PutUint32(key[i*4:], w+tid)
	}
	out := make([]byte, len(b))
	for i, v := range b {
		out[i] = v ^ key[i%32]
	}
	return out
}

// checksum is CRC-32 with polynomial 0x04c11db7, initial value 0 and no final
// xor, over the encrypted bytes. hash/crc32 complements its input and output,
// so feeding 0xffffffff gives an initial state of 0 and the output is undone.
func checksum(b []byte) uint32 {
	return ^crc32.Update(0xffffffff, crc32.IEEETable, b)
}

func (d *Discovery) Marshal() ([]byte, error) {
	if d.XML == nil {
		return nil, errors.New("udp: discovery without message")
	}
	enc := xmlCrypt(d.TID, marshalXML(d.XML))

	b := make([]byte, 20, 20+len(enc))
	binary.LittleEndian.PutUint32(b, MagicDiscovery)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(enc)))
	binary.LittleEndian.PutUint32(b[8:], 1)
	binary.LittleEndian.PutUint32(b[12:], d.TID)
	binary.LittleEndian.PutUint32(b[16:], checksum(enc))
	return append(b, enc...), nil
}

func (a *Ack) Marshal() ([]byte, error) {
	b := make([]byte, 28, 28+len(a.Payload))
	binary.LittleEndian.PutUint32(b, MagicAck)
	binary.LittleEndian.PutUint32(b[4:], uint32(a.ConnID))
	// b[8:12] unknown, 0
	binary.LittleEndian.PutUint32(b[12:], a.GroupID)
	binary.LittleEndian.PutUint32(b[16:], a.PacketID)
	binary.LittleEndian.PutUint32(b[20:], a.RecvBytesPerSec)
	binary.LittleEndian.PutUint32(b[24:], uint32(len(a.Payload)))
	return append(b, a.Payload...), nil
}

func (d *Data) Marshal() ([]byte, error) {
	b := make([]byte, 20, 20+len(d.Payload))
	binary.LittleEndian.PutUint32(b, MagicData)
	binary.LittleEndian.PutUint32(b[4:], uint32(d.ConnID))
	// b[8:12] unknown, 0
	binary.LittleEndian.PutUint32(b[12:], d.PacketID)
	binary.LittleEndian.PutUint32(b[16:], uint32(len(d.Payload)))
	return append(b, d.Payload...), nil
}

// Decode parses one datagram. Payloads are copied, b may be reused.
func Decode(b []byte) (Packet, error) {
	if len(b) < 4 {
		return nil, ErrShort
	}
	le := binary.LittleEndian

	switch le.Uint32(b) {
	case MagicDiscovery:
		if len(b) < 20 {
			return nil, ErrShort
		}
		size := le.Uint32(b[4:])
		if le.Uint32(b[8:]) != 1 {
			return nil, ErrFormat
		}
		tid, sum := le.Uint32(b[12:]), le.Uint32(b[16:])
		if uint64(len(b)-20) < uint64(size) {
			return nil, ErrShort
		}
		enc := b[20 : 20+size]
		if checksum(enc) != sum {
			return nil, ErrChecksum
		}
		msg, err := parseXML(xmlCrypt(tid, enc))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrFormat, err)
		}
		return &Discovery{TID: tid, XML: msg}, nil

	case MagicAck:
		if len(b) < 28 {
			return nil, ErrShort
		}
		a := &Ack{
			ConnID:          int32(le.Uint32(b[4:])),
			GroupID:         le.Uint32(b[12:]),
			PacketID:        le.Uint32(b[16:]),
			RecvBytesPerSec: le.Uint32(b[20:]),
		}
		if le.Uint32(b[8:]) != 0 || (a.GroupID != 0 && a.GroupID != NoAck) {
			return nil, ErrFormat
		}
		size := le.Uint32(b[24:])
		if uint64(len(b)-28) < uint64(size) {
			return nil, ErrShort
		}
		if size > 0 {
			a.Payload = append([]byte(nil), b[28:28+size]...)
		}
		return a, nil

	case MagicData:
		if len(b) < 20 {
			return nil, ErrShort
		}
		if le.Uint32(b[8:]) != 0 {
			return nil, ErrFormat
		}
		size := le.Uint32(b[16:])
		if uint64(len(b)-20) < uint64(size) {
			return nil, ErrShort
		}
		return &Data{
			ConnID:   int32(le.Uint32(b[4:])),
			PacketID: le.Uint32(b[12:]),
			Payload:  append([]byte(nil), b[20:20+size]...),
		}, nil
	}
	return nil, ErrMagic
}
