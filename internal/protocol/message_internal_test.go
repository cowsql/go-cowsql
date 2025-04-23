package protocol

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected == nil || actual == nil {
		if expected != actual {
			t.Fatal(expected, actual)
		}
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal(expected, actual)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessage_StaticBytesAlignment(t *testing.T) {
	message := Message{}
	message.Init(4096)
	pointer := uintptr(unsafe.Pointer(&message.body.Bytes[0]))
	assertEqual(t, uintptr(0), pointer%messageWordSize)
}

func TestMessage_putBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	message := Message{}
	message.Init(64)

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			message.putBlob(c.Blob)

			bytes, offset := message.Body()

			assertEqual(t, bytes[8:len(c.Blob)+8], c.Blob)
			assertEqual(t, offset, c.Offset)

			message.reset()
		})
	}
}

func TestMessage_putString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello world", 16},
	}

	message := Message{}
	message.Init(16)

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message.putString(c.String)

			bytes, offset := message.Body()

			assertEqual(t, string(bytes[:len(c.String)]), c.String)
			assertEqual(t, offset, c.Offset)

			message.reset()
		})
	}
}

func TestMessage_putUint8(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint8(12)

	message.putUint8(v)

	bytes, offset := message.Body()

	assertEqual(t, bytes[0], byte(v))

	assertEqual(t, offset, 1)
}

func TestMessage_putUint16(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint16(666)

	message.putUint16(v)

	bytes, offset := message.Body()

	assertEqual(t, bytes[0], byte((v & 0x00ff)))
	assertEqual(t, bytes[1], byte((v&0xff00)>>8))

	assertEqual(t, offset, 2)
}

func TestMessage_putUint32(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint32(130000)

	message.putUint32(v)

	bytes, offset := message.Body()

	assertEqual(t, bytes[0], byte((v & 0x000000ff)))
	assertEqual(t, bytes[1], byte((v&0x0000ff00)>>8))
	assertEqual(t, bytes[2], byte((v&0x00ff0000)>>16))
	assertEqual(t, bytes[3], byte((v&0xff000000)>>24))

	assertEqual(t, offset, 4)
}

func TestMessage_putUint64(t *testing.T) {
	message := Message{}
	message.Init(8)

	v := uint64(5000000000)

	message.putUint64(v)

	bytes, offset := message.Body()

	assertEqual(t, bytes[0], byte((v & 0x00000000000000ff)))
	assertEqual(t, bytes[1], byte((v&0x000000000000ff00)>>8))
	assertEqual(t, bytes[2], byte((v&0x0000000000ff0000)>>16))
	assertEqual(t, bytes[3], byte((v&0x00000000ff000000)>>24))
	assertEqual(t, bytes[4], byte((v&0x000000ff00000000)>>32))
	assertEqual(t, bytes[5], byte((v&0x0000ff0000000000)>>40))
	assertEqual(t, bytes[6], byte((v&0x00ff000000000000)>>48))
	assertEqual(t, bytes[7], byte((v&0xff00000000000000)>>56))

	assertEqual(t, offset, 8)
}

func TestMessage_putNamedValues(t *testing.T) {
	message := Message{}
	message.Init(256)

	timestamp, err := time.ParseInLocation("2006-01-02", "2018-08-01", time.UTC)
	requireNoError(t, err)

	values := NamedValues{
		{Ordinal: 1, Value: int64(123)},
		{Ordinal: 2, Value: float64(3.1415)},
		{Ordinal: 3, Value: true},
		{Ordinal: 4, Value: []byte{1, 2, 3, 4, 5, 6}},
		{Ordinal: 5, Value: "hello"},
		{Ordinal: 6, Value: nil},
		{Ordinal: 7, Value: timestamp},
	}

	message.putNamedValues(values)

	bytes, offset := message.Body()

	assertEqual(t, 96, offset)
	assertEqual(t, bytes[0], byte(7))
	assertEqual(t, bytes[1], byte(Integer))
	assertEqual(t, bytes[2], byte(Float))
	assertEqual(t, bytes[3], byte(Boolean))
	assertEqual(t, bytes[4], byte(Blob))
	assertEqual(t, bytes[5], byte(Text))
	assertEqual(t, bytes[6], byte(Null))
	assertEqual(t, bytes[7], byte(ISO8601))
}

func TestMessage_putNamedValues32(t *testing.T) {
	message := Message{}
	message.Init(256)

	timestamp, err := time.ParseInLocation("2006-01-02", "2018-08-01", time.UTC)
	requireNoError(t, err)

	values := NamedValues{
		{Ordinal: 1, Value: int64(123)},
		{Ordinal: 2, Value: float64(3.1415)},
		{Ordinal: 3, Value: true},
		{Ordinal: 4, Value: []byte{1, 2, 3, 4, 5, 6}},
		{Ordinal: 5, Value: "hello"},
		{Ordinal: 6, Value: nil},
		{Ordinal: 7, Value: timestamp},
	}

	message.putNamedValues32(values)

	bytes, offset := message.Body()

	assertEqual(t, 104, offset)
	assertEqual(t, bytes[0], byte(7))
	assertEqual(t, bytes[1], byte(0))
	assertEqual(t, bytes[2], byte(0))
	assertEqual(t, bytes[3], byte(0))
	assertEqual(t, bytes[4], byte(Integer))
	assertEqual(t, bytes[5], byte(Float))
	assertEqual(t, bytes[6], byte(Boolean))
	assertEqual(t, bytes[7], byte(Blob))
	assertEqual(t, bytes[8], byte(Text))
	assertEqual(t, bytes[9], byte(Null))
	assertEqual(t, bytes[10], byte(ISO8601))
}

func TestMessage_putHeader(t *testing.T) {
	message := Message{}
	message.Init(64)

	message.putString("hello")
	message.putHeader(RequestExec, 1)
}

func BenchmarkMessage_putString(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.reset()
		message.putString("hello")
	}
}

func BenchmarkMessage_putUint64(b *testing.B) {
	message := Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.reset()
		message.putUint64(270)
	}
}

func TestMessage_getString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello!!!", 16},
		{"hello world", 16},
	}

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message := Message{}
			message.Init(16)

			message.putString(c.String)
			message.putHeader(0, 0)

			message.Rewind()

			s := message.getString()

			_, offset := message.Body()

			assertEqual(t, s, c.String)
			assertEqual(t, offset, c.Offset)
		})
	}
}

func TestMessage_getBlob(t *testing.T) {
	cases := []struct {
		Blob   []byte
		Offset int
	}{
		{[]byte{1, 2, 3, 4, 5}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, 16},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 24},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d", c.Offset), func(t *testing.T) {
			message := Message{}
			message.Init(64)

			message.putBlob(c.Blob)
			message.putHeader(0, 0)

			message.Rewind()

			bytes := message.getBlob()

			_, offset := message.Body()

			assertEqual(t, bytes, c.Blob)
			assertEqual(t, offset, c.Offset)
		})
	}
}

// The overflowing string ends exactly at word boundary.
func TestMessage_getString_Overflow_WordBoundary(t *testing.T) {
	message := Message{}
	message.Init(8)

	message.putBlob([]byte{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
		'i', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
		0, 0, 0, 0, 0, 0, 0,
	})
	message.putHeader(0, 0)

	message.Rewind()
	message.getUint64()

	s := message.getString()
	assertEqual(t, "abcdefghilmnopqr", s)

	assertEqual(t, 32, message.body.Offset)
}
