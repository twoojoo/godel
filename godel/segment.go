package godel

import (
	"encoding/binary"
	"fmt"
	"io"
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

func (e *appendError) isMaxSizeReached() bool {
	if e == nil {
		return false
	}

	return e.err == errMaxSizeReached
}

type Segment struct {
	baseOffset uint64
	nextOffset uint64
	logFile    *os.File
	currSize   uint32
	maxSize    int32
	capped     bool
}

// newSegment initializes a segment by opening the file descriptor to the segment log file.
func newSegment(basePath, topicName string, partition uint32, baseOffset uint64, maxSize int32) (*Segment, error) {
	logFileName := strconv.Itoa(int(baseOffset)) + ".log"
	logFilePath := fmt.Sprintf("%s/%s/%v/%s", basePath, topicName, partition, logFileName)

	file, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		logFile:    file,
		currSize:   0,
		maxSize:    maxSize,
		capped:     false,
	}, nil
}

func loadSegment(basePath, topicName string, partition uint32, baseOffset uint64, maxSize int32) (*Segment, error) {
	segment, err := newSegment(basePath, topicName, partition, baseOffset, maxSize)
	if err != nil {
		return nil, err
	}

	err = segment.loadOffsets()
	if err != nil {
		return nil, err
	}

	return segment, nil
}

func (s *Segment) loadOffsets() error {
	pos := int64(0)
	var baseOffset uint64
	var nextOffset uint64

	for {
		messageOffsetBuf := make([]byte, 8)
		_, err := s.logFile.ReadAt(messageOffsetBuf, pos+4)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		messageSizeBuf := make([]byte, 4)
		_, err = s.logFile.ReadAt(messageSizeBuf, pos)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		messageOffset := binary.BigEndian.Uint64(messageOffsetBuf)
		messageSize := binary.BigEndian.Uint32(messageSizeBuf)

		if pos == 0 {
			baseOffset = messageOffset
		}

		nextOffset = messageOffset + 1

		pos += int64(messageSize)
	}

	s.baseOffset = baseOffset
	s.nextOffset = nextOffset
	return nil
}

func (s *Segment) close() error {
	return s.logFile.Close()
}

// append serializes the message and sets its offset based on the segment next
// available offset. Then it appends it to the segment log.
//
// Returns the message offset and an error.
func (s *Segment) append(message *Message) (uint64, *appendError) {
	message.offset = s.nextOffset
	blob := message.serialize()

	offset, appendErr := s.appendBlob(blob)
	if appendErr != nil {
		return 0, appendErr
	}

	if offset != message.offset {
		return 0, &appendError{err: errMaxSizeReached}
	}

	return offset, nil
}

// appendBlob works like Append, but requires an already serialized message.
//
// It sets the message offset in the blob but does not set the offset in the message object,
// so it must be set manually after the function is executed.
//
// Returns the message offset and an error.
func (s *Segment) appendBlob(blob []byte) (uint64, *appendError) {
	if len(blob)+int(s.currSize) > int(s.maxSize) {
		return 0, &appendError{err: errMaxSizeReached}
	}

	newOffsetBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(newOffsetBuf, s.nextOffset)

	copy(blob[4:12], newOffsetBuf)

	_, err := s.logFile.Write(blob)
	if err != nil {
		return 0, &appendError{err: err.Error()}
	}

	offset := s.nextOffset
	s.nextOffset++

	return offset, nil
}

// getMessage efficiently scans the log file to extract the message at the given offset.
//
// Returns a *Message and an error
func (s *Segment) getMessage(offset uint64) (*Message, error) {
	pos := int64(0)

	for {
		messageOffsetBuf := make([]byte, 8)
		_, err := s.logFile.ReadAt(messageOffsetBuf, pos+4)
		if err != nil {
			return nil, err
		}

		messageSizeBuf := make([]byte, 4)
		_, err = s.logFile.ReadAt(messageSizeBuf, pos)
		if err != nil {
			return nil, err
		}

		messageOffset := binary.BigEndian.Uint64(messageOffsetBuf)
		messageSize := binary.BigEndian.Uint32(messageSizeBuf)

		if messageOffset == offset {
			messageBuf := make([]byte, messageSize)
			_, err = s.logFile.ReadAt(messageBuf, pos)
			if err != nil {
				return nil, err
			}

			msg, err := deserializeMessage(messageBuf)
			if err != nil {
				return msg, err
			}

			return msg, nil
		}

		pos += int64(messageSize)
	}
}
