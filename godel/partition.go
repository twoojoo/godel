package godel

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

const errPartitionAlreadyExists = "partition.already.exists"

type Partition struct {
	newMessageCh  chan int
	ID            uint32
	Segments      []*Segment // guaranteed segments order by offset
	TopicOptions  *TopicOptions
	BrokerOptions *BrokerOptions
	TopicName     string
}

func newPartition(id uint32, topicName string, topicOptions *TopicOptions, brokerOptions *BrokerOptions) (*Partition, error) {
	partitionPath := fmt.Sprintf("%s/%s/%v", brokerOptions.BasePath, topicName, id)

	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		err = os.Mkdir(partitionPath, 0755)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		return nil, errors.New(errPartitionAlreadyExists)
	}

	slog.Info("initializing partition", "topic", topicName, "partition", id)

	return &Partition{
		ID:            id,
		newMessageCh:  make(chan int),
		TopicName:     topicName,
		TopicOptions:  topicOptions,
		BrokerOptions: brokerOptions,
	}, nil
}

func loadPartition(id uint32, topicName string, topicOptions *TopicOptions, brokerOptions *BrokerOptions) (*Partition, error) {
	slog.Info("loading partition", "topic", topicName, "partition", id)

	partitionPath := fmt.Sprintf("%s/%s/%v", brokerOptions.BasePath, topicName, id)

	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		err = os.Mkdir(partitionPath, 0755)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	segmentsStr, err := listFilesFilterFormat(partitionPath, "log")
	if err != nil {
		return nil, err
	}

	segmentsBaseOffsets, err := strSliceToUint64(segmentsStr)
	if err != nil {
		return nil, err
	}

	segments := make([]*Segment, 0, len(segmentsBaseOffsets))
	for i := range segmentsBaseOffsets {
		segment, err := loadSegment(brokerOptions.BasePath, topicName, id, segmentsBaseOffsets[i], topicOptions.SegmentBytes)
		if err != nil {
			return nil, err
		}

		segments = append(segments, segment)
	}

	return &Partition{
		ID:            id,
		newMessageCh:  make(chan int),
		TopicName:     topicName,
		TopicOptions:  topicOptions,
		BrokerOptions: brokerOptions,
		Segments:      segments,
	}, nil
}

func (p *Partition) GetBaseOffset() uint64 {
	if len(p.Segments) == 0 {
		return 0
	}

	return p.Segments[0].BaseOffset
}

func (p *Partition) GetNextOffset() uint64 {
	if len(p.Segments) == 0 {
		return 1
	}

	return p.Segments[len(p.Segments)-1].NextOffset
}

func (p *Partition) push(message *Message) (uint64, error) {
	blob := message.Serialize()

	// check that blob size doesn't exceed max message size
	if len(blob) > int(p.TopicOptions.SegmentBytes) {
		return 0, fmt.Errorf("message.exceeds.max.segment.size")
	}

	// create new segment if none
	if len(p.Segments) == 0 {
		slog.Info("initializing new segment", "base_offset", 0)

		firstSegment, err := newSegment(p.BrokerOptions.BasePath, p.TopicName, p.ID, 0, p.TopicOptions.SegmentBytes)
		if err != nil {
			return 0, err
		}

		p.Segments = append(p.Segments, firstSegment)
	}

	// append the message to the log segment
	offset, appendErr := p.Segments[len(p.Segments)-1].AppendBlob(blob)
	if appendErr.IsMaxSizeReached() {
		// if the max size of the segment is reached, create a new one
		nextOffset := p.GetNextOffset()
		newSegment, err := newSegment(p.BrokerOptions.BasePath, p.TopicName, p.ID, nextOffset, p.TopicOptions.SegmentBytes)
		if err != nil {
			return 0, err
		}
		p.Segments = append(p.Segments, newSegment)
	} else if appendErr != nil {
		return 0, appendErr
	}

	message.Offset = offset
	return offset, nil
}

func (p *Partition) Consume(offset uint64, callback func(message *Message) error) error {
	// seatch the segment corresponding to the requested offset
	segmentIdx := binarySearchSegment(p.Segments, offset)

	// if requested offset is less than first available offset
	// default to first available offset
	if segmentIdx == -1 {
		segmentIdx = 0
		offset = p.Segments[0].BaseOffset
	}

	// start consume loop
	for {
		// if there are no segment, or the requested offset is after the last segment.
		// then wait for a new message before continuing
		if len(p.Segments) == 0 || segmentIdx >= len(p.Segments) {
			<-p.newMessageCh
			continue
		}

		// if the requested offset is inferior to the current segment next offset
		// (aka if the message actually exists) get the message, otherwise just
		// wait for a new message signal.
		if offset < p.Segments[segmentIdx].NextOffset {
			message, err := p.Segments[segmentIdx].GetMessage(offset)

			// last message segment reached and segment capped
			// increment segmentindex and continue looping
			if err == io.EOF && p.Segments[segmentIdx].Capped {
				segmentIdx++
				continue
			}

			// last message segment reached and segment not capped
			// (just wait for a new message)
			if err == io.EOF && !p.Segments[segmentIdx].Capped {
				<-p.newMessageCh
				continue
			}

			if err != nil {
				return err
			}

			// execute calback on message
			err = callback(message)
			if err != nil {
				return err
			}

			// everything went right so we
			// can increment consumer offset
			offset++
		} else {
			<-p.newMessageCh
		}
	}
}

// binarySearchSegment search for the segment containing the requested offset.
//
// If the segment is found, its index is returned.
//
// If the requested offset is inferior to the first available segment, then it returns -1.
//
// If it's superior to the maximum available segment, it returns the last segment index + 1.
func binarySearchSegment(segments []*Segment, offset uint64) int {
	return bsSegment(segments, offset, 0, len(segments))
}

func bsSegment(segments []*Segment, offset uint64, lo, hi int) int {
	maxIndex := hi - 1

	if maxIndex >= len(segments) || maxIndex < 0 {
		return maxIndex + 1
	}

	maxSegment := segments[maxIndex]

	// offset is inside this segment
	if offset >= maxSegment.BaseOffset && offset < maxSegment.NextOffset {
		return maxIndex
	}

	// offset is last message of ths non-capped segment
	if offset >= maxSegment.BaseOffset && offset == maxSegment.NextOffset && !maxSegment.Capped {
		return maxIndex
	}

	// offset is first message of the next segment (if exists)
	if offset >= maxSegment.BaseOffset && offset >= maxSegment.NextOffset {
		return maxIndex + 1
	}

	// canì't divide anymore
	if lo == hi-1 {
		return -1
	}

	// offset is in the high side
	if offset > maxSegment.BaseOffset {
		return bsSegment(segments, offset, hi, len(segments))
	}

	// offset is in the low side
	mid := (hi-lo)/2 + 1
	return bsSegment(segments, offset, lo, lo+mid)
}
