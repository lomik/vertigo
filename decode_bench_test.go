package vertigo

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func decodeNumericV0(reader io.Reader, data interface{}) error {
	return binary.Read(reader, binary.BigEndian, data)
}

func BenchmarkUint32V0(b *testing.B) {
	var raw = []byte("\x01u\xcb\xea") // encoded 24497130

	var value uint32

	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer(raw)
		decodeNumericV0(buf, &value)
		if value != 24497130 {
			b.FailNow()
		}
	}
}

func BenchmarkUint32V0WithBufio(b *testing.B) {
	var raw = []byte("\x01u\xcb\xea") // encoded 24497130

	var value uint32

	for n := 0; n < b.N; n++ {
		buf := bufio.NewReader(bytes.NewBuffer(raw))
		decodeNumericV0(buf, &value)
		if value != 24497130 {
			b.FailNow()
		}
	}
}

func unpackUint32V1(p []byte) uint32 {
	result := uint32(0)
	for i := uint(0); i < 4; i++ {
		result |= uint32(p[i]) << (8 * (3 - i))
	}
	return result
}

func BenchmarkUnpackUint32V1(b *testing.B) {
	var raw = []byte("\x01u\xcb\xea") // encoded 24497130
	var value uint32

	for n := 0; n < b.N; n++ {
		if len(raw) != 4 {
			b.FailNow()
		}
		value = unpackUint32V1(raw)
		if value != 24497130 {
			b.FailNow()
		}
	}
}

func BenchmarkDecodeUint32(b *testing.B) {
	var raw = []byte("\x01u\xcb\xea") // encoded 24497130
	var value uint32

	for n := 0; n < b.N; n++ {
		decodeUint32(raw, &value)
		if value != 24497130 {
			b.FailNow()
		}
	}
}
