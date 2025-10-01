package main

import (
	"encoding/binary"
	"fmt"
)

type Message struct {
	Offset    uint64
	Key       []byte
	Payload   []byte
	Timestamp uint64
}

func NewMessage(timestamp uint64, key []byte, payload []byte) *Message {
	return &Message{
		Offset:    0,
		Key:       key,
		Payload:   payload,
		Timestamp: timestamp,
	}
}

func DeserializeMessage(b []byte) (*Message, error) {
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
		Offset:    offset,
		Key:       key,
		Payload:   payload,
		Timestamp: timestamp,
	}, nil
}

func (m *Message) Serialize() []byte {
	totalSize := uint32(4 + // total size itself
		8 + // offset
		8 + // timestamp
		4 + // key length
		// 4 + // payload length
		len([]byte(m.Key)) + // key
		len(m.Payload)) // payload

	totalSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalSizeBytes, totalSize)

	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, m.Offset)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, m.Timestamp)

	keyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBytes, uint32(len(m.Key)))

	// payloadSizeBytes := make([]byte, 4)
	// binary.BigEndian.PutUint32(payloadSizeBytes, uint32(len(m.Payload)))

	blob := make([]byte, 0, totalSize)
	blob = append(blob, totalSizeBytes...)
	blob = append(blob, offsetBytes...)
	blob = append(blob, timestampBytes...)
	blob = append(blob, keyLenBytes...)
	// blob = append(blob, payloadSizeBytes...)
	blob = append(blob, m.Key...)
	blob = append(blob, m.Payload...)

	return blob
}
