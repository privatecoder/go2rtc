package bc

import (
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func readTestdata(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestXOR(t *testing.T) {
	sample := readTestdata(t, "xml_crypto_sample1.bin")
	plain := readTestdata(t, "xml_crypto_sample1_plaintext.bin")

	c := Cipher{Mode: ModeBC}
	require.Equal(t, plain, c.Decrypt(0, sample))
	require.Equal(t, sample, c.Encrypt(0, plain))
}

func TestXORRoundTripOffsets(t *testing.T) {
	c := Cipher{Mode: ModeBC}
	data := make([]byte, 256)
	for off := uint32(0); off < 256; off += 7 {
		require.Equal(t, data, c.Decrypt(off, c.Encrypt(off, data)))
	}
	// a nonzero offset changes the stream
	require.NotEqual(t, c.Encrypt(0, data), c.Encrypt(1, data))
}

func TestXORKnownOffset(t *testing.T) {
	// first bytes of an xml payload (channel_id 138) from neolink's b800 samples
	enc := readTestdata(t, "xml_mainstream_b800.bin")[24:]
	dec := Cipher{Mode: ModeBC}.Decrypt(138, enc)
	require.Contains(t, string(dec), "<streamType>mainStream</streamType>")
}

func TestNone(t *testing.T) {
	c := Cipher{}
	require.Equal(t, []byte("abc"), c.Decrypt(5, []byte("abc")))
	require.Equal(t, []byte("abc"), c.Encrypt(5, []byte("abc")))
}

func TestModernHash(t *testing.T) {
	require.Equal(t, "9F07915E819A076E2E14169830769D6", ModernHash("admin"+"9E6D1FCB9E69846D"))
	require.Len(t, ModernHash("x"), 31)
	require.Equal(t, "21232F297A57A5A743894A0E4A801FC\x00", LegacyHash("admin"))
}

func TestAESKey(t *testing.T) {
	key := AESKey("9E6D1FCB9E69846D", "123456")
	require.Equal(t, "0D119EB629684BDF", string(key[:]))
}

func TestAESVector(t *testing.T) {
	c := NewAESCipher(ModeAES, "9E6D1FCB9E69846D", "123456")
	plain := []byte(`<?xml version="1.0" encoding="UTF-8" ?><body><Preview/></body>`)
	want := "c640e9ad98c8769f3cdb1a55cfd4985710bee2e828f6d0d6cd7b76bc8c7cc3fe19a62165915a810dd1f9842104dbe1b4147050eee074ee5eb1228d9cd8e9"

	enc := c.Encrypt(0, plain)
	require.Equal(t, want, hex.EncodeToString(enc))
	require.Equal(t, plain, c.Decrypt(0, enc))

	// the offset is ignored by AES, a fresh IV is used per message
	require.Equal(t, enc, c.Encrypt(77, plain))
}
