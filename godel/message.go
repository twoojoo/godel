package godel

import (
	"encoding/binary"
	"fmt"
)

type Message struct {
	partition uint32
	offset    uint64
	key       []byte
	payload   []byte
	timestamp uint64
}

func NewMessage(timestamp uint64, key []byte, payload []byte) *Message {
	return &Message{
		offset:    0,
		key:       key,
		payload:   payload,
		timestamp: timestamp,
	}
}

func deserializeMessage(b []byte) (*Message, error) {
	messageSize := binary.BigEndian.Uint32(b)

	if messageSize != uint32(len(b)) {
		return nil, fmt.Errorf("message.size.mismatch")
	}

	offset := binary.BigEndian.Uint64(b[4:12])
	timestamp := binary.BigEndian.Uint64(b[12:20])
	keyLen := binary.BigEndian.Uint32(b[20:24])

	key := b[24 : 24+keyLen]
	payload := b[24+keyLen : messageSize]

	return &Message{
		offset:    offset,
		key:       key,
		payload:   payload,
		timestamp: timestamp,
	}, nil
}

func (m *Message) serialize() []byte {
	totalSize := uint32(4 + // total size itself
		8 + // offset
		8 + // timestamp
		4 + // key length
		// 4 + // payload length
		len([]byte(m.key)) + // key
		len(m.payload)) // payload

	totalSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalSizeBytes, totalSize)

	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, m.offset)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, m.timestamp)

	keyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBytes, uint32(len(m.key)))

	// payloadSizeBytes := make([]byte, 4)
	// binary.BigEndian.PutUint32(payloadSizeBytes, uint32(len(m.Payload)))

	blob := make([]byte, 0, totalSize)
	blob = append(blob, totalSizeBytes...)
	blob = append(blob, offsetBytes...)
	blob = append(blob, timestampBytes...)
	blob = append(blob, keyLenBytes...)
	// blob = append(blob, payloadSizeBytes...)
	blob = append(blob, m.key...)
	blob = append(blob, m.payload...)

	return blob
}
