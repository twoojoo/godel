package broker

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
)

const errPartitionAlreadyExists = "partition.already.exists"

type Partition struct {
	newMessageCh  chan int
	num           uint32
	segments      []*Segment // guaranteed segments order by offset
	topicOptions  *TopicOptions
	brokerOptions *BrokerOptions
	topicName     string
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
		num:           id,
		newMessageCh:  make(chan int),
		topicName:     topicName,
		topicOptions:  topicOptions,
		brokerOptions: brokerOptions,
	}, nil
}

func loadPartition(id uint32, topicName string, topicOptions *TopicOptions, brokerOptions *BrokerOptions) (*Partition, error) {
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
		num:           id,
		newMessageCh:  make(chan int),
		topicName:     topicName,
		topicOptions:  topicOptions,
		brokerOptions: brokerOptions,
		segments:      segments,
	}, nil
}

func (p *Partition) getBaseOffset() uint64 {
	if len(p.segments) == 0 {
		return 0
	}

	return p.segments[0].baseOffset
}

func (p *Partition) getNextOffset() uint64 {
	if len(p.segments) == 0 {
		return 1
	}

	return p.segments[len(p.segments)-1].nextOffset
}

func (p *Partition) getSize() uint32 {
	var size uint32
	for i := range p.segments {
		size += p.segments[i].currSize
	}

	return size
}

func (p *Partition) push(message *Message) (uint64, error) {
	blob := message.serialize()

	// check that blob size doesn't exceed max message size
	if len(blob) > int(p.topicOptions.SegmentBytes) {
		return 0, fmt.Errorf("message.exceeds.max.segment.size")
	}

	// create new segment if none
	if len(p.segments) == 0 {
		slog.Info("initializing new segment", "base_offset", 0)

		firstSegment, err := newSegment(p.brokerOptions.BasePath, p.topicName, p.num, 0, p.topicOptions.SegmentBytes)
		if err != nil {
			return 0, err
		}

		p.segments = append(p.segments, firstSegment)
	}

	// append the message to the log segment
	offset, appendErr := p.segments[len(p.segments)-1].appendBlob(blob)
	if appendErr.isMaxSizeReached() {
		// if the max size of the segment is reached, create a new one
		nextOffset := p.getNextOffset()
		newSegment, err := newSegment(p.brokerOptions.BasePath, p.topicName, p.num, nextOffset, p.topicOptions.SegmentBytes)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, newSegment)
	} else if appendErr != nil {
		return 0, appendErr
	}

	message.offset = offset
	return offset, nil
}

func (p *Partition) consume(offset uint64, callback func(message *Message) error) error {
	// seatch the segment corresponding to the requested offset
	segmentIdx := binarySearchSegment(p.segments, offset)

	// if requested offset is less than first available offset
	// default to first available offset
	if segmentIdx == -1 {
		segmentIdx = 0
		offset = p.segments[0].baseOffset
	}

	// start consume loop
	for {
		// if there are no segment, or the requested offset is after the last segment.
		// then wait for a new message before continuing
		if len(p.segments) == 0 || segmentIdx >= len(p.segments) {
			<-p.newMessageCh
			continue
		}

		// if the requested offset is inferior to the current segment next offset
		// (aka if the message actually exists) get the message, otherwise just
		// wait for a new message signal.
		if offset < p.segments[segmentIdx].nextOffset {
			message, err := p.segments[segmentIdx].getMessage(offset)

			// last message segment reached and segment capped
			// increment segmentindex and continue looping
			if err == io.EOF && p.segments[segmentIdx].capped {
				segmentIdx++
				continue
			}

			// last message segment reached and segment not capped
			// (just wait for a new message)
			if err == io.EOF && !p.segments[segmentIdx].capped {
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
	if offset >= maxSegment.baseOffset && offset < maxSegment.nextOffset {
		return maxIndex
	}

	// offset is last message of ths non-capped segment
	if offset >= maxSegment.baseOffset && offset == maxSegment.nextOffset && !maxSegment.capped {
		return maxIndex
	}

	// offset is first message of the next segment (if exists)
	if offset >= maxSegment.baseOffset && offset >= maxSegment.nextOffset {
		return maxIndex + 1
	}

	// canì't divide anymore
	if lo == hi-1 {
		return -1
	}

	// offset is in the high side
	if offset > maxSegment.baseOffset {
		return bsSegment(segments, offset, hi, len(segments))
	}

	// offset is in the low side
	mid := (hi-lo)/2 + 1
	return bsSegment(segments, offset, lo, lo+mid)
}

func (p *Partition) deleteSegment(i int) error {
	err := p.segments[i].delete(p.brokerOptions.BasePath, p.topicName, p.num)
	if err != nil {
		return err
	}

	p.segments = slices.Delete(p.segments, i, i)
	return nil
}
