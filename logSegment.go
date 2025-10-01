package main

import (
	"encoding/binary"
	"os"
	"strconv"
)

const errMaxSizeReached = "max.segment.size.reached"

type appendError struct {
	err string
}

func (e *appendError) Error() string {
	return e.err
}

func (e *appendError) IsMaxSizeReached() bool {
	if e == nil {
		return false
	}

	return e.err == errMaxSizeReached
}

type Segment struct {
	BaseOffset uint64
	NextOffset uint64
	LogFile    *os.File
	CurrSize   uint32
	MaxSize    uint32
	Capped     bool
}

// NewSegment initializes a segment by opening the file descriptor to the segment log file.
func NewSegment(baseOffset uint64, maxSize uint32) (*Segment, error) {
	logFilePath := strconv.Itoa(int(baseOffset)) + ".log"
	file, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		LogFile:    file,
		CurrSize:   0,
		MaxSize:    maxSize,
		Capped:     false,
	}, nil
}

func (s *Segment) Close() error {
	return s.LogFile.Close()
}

// Append serializes the message and sets its offset based on the segment next
// available offset. Then it appends it to the segment log.
//
// Returns the message offset and an error.
func (s *Segment) Append(message *Message) (uint64, *appendError) {
	message.Offset = s.NextOffset
	blob := message.Serialize()

	offset, appendErr := s.AppendBlob(blob)
	if appendErr != nil {
		return 0, appendErr
	}

	if offset != message.Offset {
		return 0, &appendError{err: errMaxSizeReached}
	}

	return offset, nil
}

// AppendBlob works like Append, but requires an already serialized message.
//
// It sets the message offset in the blob but does not set the offset in the message object,
// so it must be set manually after the function is executed.
//
// Returns the message offset and an error.
func (s *Segment) AppendBlob(blob []byte) (uint64, *appendError) {
	if len(blob)+int(s.CurrSize) > int(s.MaxSize) {
		return 0, &appendError{err: errMaxSizeReached}
	}

	newOffsetBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(newOffsetBuf, s.NextOffset)

	copy(blob[4:12], newOffsetBuf)

	_, err := s.LogFile.Write(blob)
	if err != nil {
		return 0, &appendError{err: err.Error()}
	}

	offset := s.NextOffset
	s.NextOffset++

	return offset, nil
}

// GetMessage efficiently scans the log file to extract the message at the given offset.
//
// Returns a *Message and an error
func (s *Segment) GetMessage(offset uint64) (*Message, error) {
	pos := int64(0)

	for {
		messageOffsetBuf := make([]byte, 8)
		_, err := s.LogFile.ReadAt(messageOffsetBuf, pos+4)
		if err != nil {
			return nil, err
		}

		messageSizeBuf := make([]byte, 4)
		_, err = s.LogFile.ReadAt(messageSizeBuf, pos)
		if err != nil {
			return nil, err
		}

		messageOffset := binary.BigEndian.Uint64(messageOffsetBuf)
		messageSize := binary.BigEndian.Uint32(messageSizeBuf)

		if messageOffset == offset {
			messageBuf := make([]byte, messageSize)
			_, err = s.LogFile.ReadAt(messageBuf, pos)
			if err != nil {
				return nil, err
			}

			msg, err := DeserializeMessage(messageBuf)
			if err != nil {
				return msg, err
			}

			return msg, nil
		}

		pos += int64(messageSize)
	}
}
