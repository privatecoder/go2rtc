package bc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"strings"
)

// Mode is the encryption level negotiated during login. Only AES and FullAES
// carry a key.
type Mode uint8

const (
	ModeNone    Mode = iota // older cameras, plaintext
	ModeBC                  // fixed-key XOR, offset by the header channel_id
	ModeAES                 // AES-128-CFB on the control channel
	ModeFullAES             // AES-128-CFB on control and media
)

var xorKey = [8]byte{0x1F, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0xFF}

const aesIV = "0123456789abcdef"

// Cipher encrypts and decrypts XML bodies for one connection.
type Cipher struct {
	Mode Mode
	Key  [16]byte // ModeAES and ModeFullAES only
}

// Decrypt returns a decrypted copy of b. offset is the header channel_id.
func (c Cipher) Decrypt(offset uint32, b []byte) []byte {
	out := make([]byte, len(b))
	switch c.Mode {
	case ModeBC:
		xorBC(out, b, offset)
	case ModeAES, ModeFullAES:
		cipher.NewCFBDecrypter(c.block(), []byte(aesIV)).XORKeyStream(out, b)
	default:
		copy(out, b)
	}
	return out
}

// Encrypt returns an encrypted copy of b. offset is the header channel_id.
func (c Cipher) Encrypt(offset uint32, b []byte) []byte {
	out := make([]byte, len(b))
	switch c.Mode {
	case ModeBC:
		xorBC(out, b, offset)
	case ModeAES, ModeFullAES:
		cipher.NewCFBEncrypter(c.block(), []byte(aesIV)).XORKeyStream(out, b)
	default:
		copy(out, b)
	}
	return out
}

func (c Cipher) block() cipher.Block {
	b, _ := aes.NewCipher(c.Key[:]) // key length is fixed, cannot fail
	return b
}

func xorBC(dst, src []byte, offset uint32) {
	for i, v := range src {
		dst[i] = v ^ xorKey[(i+int(offset))%8] ^ byte(offset)
	}
}

// NewAESCipher builds an AES or FullAES cipher from the login nonce and the
// account password.
func NewAESCipher(mode Mode, nonce, password string) Cipher {
	return Cipher{Mode: mode, Key: AESKey(nonce, password)}
}

// AESKey derives the AES-128 key: first 16 chars of the uppercase hex MD5 of
// "<nonce>-<password>".
func AESKey(nonce, password string) (key [16]byte) {
	copy(key[:], md5Hex(nonce+"-"+password))
	return
}

// ModernHash is the 31-char uppercase hex MD5 sent as userName/password in the
// modern login (the camera only compares 31 chars). Pass user+nonce or
// password+nonce.
func ModernHash(s string) string {
	return md5Hex(s)[:31]
}

// LegacyHash is the 32-byte field of the legacy login: 31 hex chars and a NUL.
func LegacyHash(s string) string {
	return md5Hex(s)[:31] + "\x00"
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}
