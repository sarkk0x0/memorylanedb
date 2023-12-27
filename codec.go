package memorylanedb

import (
	"bufio"
	"encoding/binary"
	"io"
)

var byteOrder = binary.LittleEndian

type Codec struct {
	w *bufio.Writer
	r io.Reader
}

func NewCodec(f io.ReadWriter) *Codec {
	return &Codec{
		w: bufio.NewWriter(f),
		r: bufio.NewReader(f),
	}
}

func (c *Codec) EncodeEntry(entry *Entry) (int64, error) {
	if entry == nil {
		return 0, ErrorNilEncoding
	}
	prefixSize := entry.HeaderSize()
	prefixBuffer := make([]byte, prefixSize)
	byteOrder.PutUint32(prefixBuffer[:CRC_SIZE], entry.Checksum)
	byteOrder.PutUint32(prefixBuffer[CRC_SIZE:CRC_SIZE+TSSTAMP_SIZE], entry.Tstamp)
	byteOrder.PutUint16(prefixBuffer[CRC_SIZE+TSSTAMP_SIZE:CRC_SIZE+TSSTAMP_SIZE+KEY_SIZE], entry.KeySize)
	byteOrder.PutUint32(prefixBuffer[CRC_SIZE+TSSTAMP_SIZE+KEY_SIZE:], entry.ValueSize)

	_, err := c.w.Write(prefixBuffer)
	if err != nil {
		return 0, ErrWritingPrefix
	}

	_, err = c.w.Write(entry.Key)
	if err != nil {
		return 0, ErrWritingKey
	}

	_, err = c.w.Write(entry.Value)
	if err != nil {
		return 0, ErrWritingValue
	}

	if flushErr := c.w.Flush(); flushErr != nil {
		return 0, flushErr
	}
	return entry.Size(), nil
}

func (c *Codec) DecodeEntry(entry *Entry) (int64, error) {
	if entry == nil {
		return 0, ErrorNilDecoding
	}
	prefixSize := entry.HeaderSize()
	prefixBuffer := make([]byte, prefixSize)

	_, err := io.ReadFull(c.r, prefixBuffer)
	if err != nil {
		return 0, err
	}
	var ptr uint32 = 0

	entry.Checksum = byteOrder.Uint32(prefixBuffer[ptr : ptr+CRC_SIZE])
	ptr += CRC_SIZE

	entry.Tstamp = byteOrder.Uint32(prefixBuffer[ptr : ptr+TSSTAMP_SIZE])
	ptr += TSSTAMP_SIZE

	entry.KeySize = byteOrder.Uint16(prefixBuffer[ptr : ptr+KEY_SIZE])
	ptr += KEY_SIZE

	entry.ValueSize = byteOrder.Uint32(prefixBuffer[ptr : ptr+VALUE_SIZE])
	ptr += VALUE_SIZE

	keyBuf := make([]byte, entry.KeySize)
	_, err = io.ReadFull(c.r, keyBuf)
	if err != nil {
		return 0, err
	}
	entry.Key = keyBuf

	valueBuf := make([]byte, entry.ValueSize)
	_, err = io.ReadFull(c.r, valueBuf)
	if err != nil {
		return 0, err
	}
	entry.Value = valueBuf

	return entry.Size(), nil
}

func (c *Codec) DecodeSingleEntry(buf []byte, entry *Entry) (int64, error) {
	if entry == nil {
		return 0, ErrorNilDecoding
	}
	var ptr uint32 = 0
	entry.Checksum = byteOrder.Uint32(buf[ptr : ptr+CRC_SIZE])
	ptr += CRC_SIZE

	entry.Tstamp = byteOrder.Uint32(buf[ptr : ptr+TSSTAMP_SIZE])
	ptr += TSSTAMP_SIZE

	entry.KeySize = byteOrder.Uint16(buf[ptr : ptr+KEY_SIZE])
	ptr += KEY_SIZE

	entry.ValueSize = byteOrder.Uint32(buf[ptr : ptr+VALUE_SIZE])
	ptr += VALUE_SIZE

	bufWithoutPrefix := buf[ptr:]

	entry.Key = bufWithoutPrefix[:entry.KeySize]
	entry.Value = bufWithoutPrefix[entry.KeySize:]

	return entry.Size(), nil
}

func (c *Codec) EncodeHint(hint *Hint) (int64, error) {
	if hint == nil {
		return 0, ErrorNilEncoding
	}
	prefixSize := hint.HeaderSize()
	prefixBuffer := make([]byte, prefixSize)
	byteOrder.PutUint32(prefixBuffer[:TSSTAMP_SIZE], hint.Tstamp)
	byteOrder.PutUint16(prefixBuffer[TSSTAMP_SIZE:TSSTAMP_SIZE+KEY_SIZE], hint.KeySize)
	byteOrder.PutUint32(prefixBuffer[TSSTAMP_SIZE+KEY_SIZE:TSSTAMP_SIZE+KEY_SIZE+VALUE_SIZE], hint.ValueSize)
	byteOrder.PutUint32(prefixBuffer[prefixSize-VALUE_OFFSET_SIZE:], hint.ValueOffset)

	_, err := c.w.Write(prefixBuffer)
	if err != nil {
		return 0, ErrWritingPrefix
	}

	_, err = c.w.Write(hint.Key)
	if err != nil {
		return 0, ErrWritingValue
	}
	if flushErr := c.w.Flush(); flushErr != nil {
		return 0, flushErr
	}
	return hint.Size(), nil
}

func (c *Codec) DecodeHint(hint *Hint) error {
	if hint == nil {
		return ErrorNilDecoding
	}
	prefixSize := hint.HeaderSize()
	prefixBuffer := make([]byte, prefixSize)

	_, err := io.ReadFull(c.r, prefixBuffer)
	if err != nil {
		return err
	}
	var ptr uint32 = 0

	hint.Tstamp = byteOrder.Uint32(prefixBuffer[ptr : ptr+TSSTAMP_SIZE])
	ptr += TSSTAMP_SIZE

	hint.KeySize = byteOrder.Uint16(prefixBuffer[ptr : ptr+KEY_SIZE])
	ptr += KEY_SIZE

	hint.ValueSize = byteOrder.Uint32(prefixBuffer[ptr : ptr+VALUE_SIZE])
	ptr += VALUE_SIZE

	hint.ValueOffset = byteOrder.Uint32(prefixBuffer[ptr : ptr+VALUE_OFFSET_SIZE])
	ptr += VALUE_OFFSET_SIZE

	keyBuf := make([]byte, hint.KeySize)
	_, err = io.ReadFull(c.r, keyBuf)
	if err != nil {
		return err
	}
	hint.Key = keyBuf

	return nil
}
