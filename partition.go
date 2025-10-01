package main

import (
	"fmt"
	"io"
)

type Partition struct {
	newMessageCh   chan int
	ID             int
	Segments       []*Segment // guaranteed segments order by offset
	MaxSegmentSize uint32
}

func NewPartition(id int, maxSegmentSize uint32) *Partition {
	return &Partition{
		ID:             id,
		MaxSegmentSize: maxSegmentSize,
		newMessageCh:   make(chan int),
	}
}

func (p *Partition) Push(message *Message) (uint64, error) {
	blob := message.Serialize()

	// check that blob size doesn't exceed max message size
	if len(blob) > int(p.MaxSegmentSize) {
		return 0, fmt.Errorf("message.exceeds.max.segment.size")
	}

	// create new segment if none
	if len(p.Segments) == 0 {
		firstSegment, err := NewSegment(0, p.MaxSegmentSize)
		if err != nil {
			return 0, err
		}
		p.Segments = append(p.Segments, firstSegment)
	}

	// append the message to the log segment
	offset, appendErr := p.Segments[len(p.Segments)-1].AppendBlob(blob)
	if appendErr.IsMaxSizeReached() {
		// if the max size of the segment is reached, create a new one
		newSegment, err := NewSegment(0, p.MaxSegmentSize)
		if err != nil {
			return 0, err
		}
		p.Segments = append(p.Segments, newSegment)
	} else if appendErr != nil {
		return 0, appendErr
	}

	message.Offset = offset
	p.newMessageCh <- 1

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
