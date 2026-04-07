package partition

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/binhdoitsme/pubsub/core"
)

const (
	maxMessagesInMemory = 10_000
)

type Partition struct {
	ID             int
	TopicName      string
	offset         uint64
	memBuf         []core.Message
	lock           sync.RWMutex
	segments       map[uint64]*LogSegment
	currentSegment *LogSegment
	basePath       string
}

func New(topicName string, index int, basePath string) *Partition {
	return &Partition{
		ID:        index,
		TopicName: topicName,
		segments:  make(map[uint64]*LogSegment),
		basePath:  basePath,
	}
}

// Append adds a message to the partition and returns its offset.
// Mimics Kafka: ordered, append‑only; offset is assigned by the partition.
func (p *Partition) Append(msg *core.Message) (offset uint64, err error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	msg.PartitionID = p.ID
	msg.Offset = p.offset

	p.memBuf = append(p.memBuf, *msg)
	p.offset++

	return msg.Offset, nil
}

func (p *Partition) findSegment(offset uint64) *LogSegment {
	var output *LogSegment
	for k, s := range p.segments {
		if output == nil || (k <= offset && k+s.NumRecords >= offset) {
			output = s
		}
	}
	return output
}

// Read reads messages from a given offset up to some limit.
// Returns messages whose offset is in [fromOffset, fromOffset + limit).
// This is a simplified Kafka‑style fetch.
func (p *Partition) Read(fromOffset, limit uint64) []core.Message {
	p.lock.RLock()
	defer p.lock.RUnlock()

	var msgs []core.Message
	// if [fromOffset, fromOffset+limit] outside of [offset-len(memBuf), offset]
	// then get from persisted files
	for i := range limit {
		currentOffset := fromOffset + i
		segment := p.findSegment(currentOffset)
		if segment == nil {
			// read from memBuf
			if len(p.memBuf) == 0 {
				limit -= i
				break
			}
			index := currentOffset - (p.offset - uint64(len(p.memBuf)))
			if index >= uint64(len(p.memBuf)) {
				limit -= i
				break
			}
			msgs = append(msgs, p.memBuf[int(index)])
		} else {
			// read from segment
			res, _ := segment.Read(ReadRequest{StartOffset: currentOffset, MaxMessages: 1})
			msgs = append(msgs, res.Messages...)
		}
	}

	// else get from memBuf
	for _, msg := range p.memBuf {
		if msg.Offset > fromOffset+limit {
			break
		}
		if msg.Offset < fromOffset {
			continue
		}
		if uint64(len(msgs)) >= limit {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// Size returns the number of messages in this partition.
func (p *Partition) Size() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return int(p.offset + 1)
}

func (p *Partition) Flush() error {
	if len(p.memBuf) == 0 {
		return nil
	}
	p.lock.RLock()
	defer p.lock.RUnlock()
	// 0. If no segment, create one
	if p.currentSegment == nil {
		var offset uint64 = p.offset - uint64(len(p.memBuf))
		p.currentSegment = CreateOrRestoreSegment(p.basePath, p.TopicName, p.ID, offset)
		p.segments[p.offset] = p.currentSegment
	}
	for {
		// write to current segment. If error and offset = nil then return error
		offset, err := p.currentSegment.WriteBatch(p.memBuf)
		if err != nil && offset == nil {
			return err
		}
		if err == nil {
			break
		}

		// else, roll out new segment
		p.currentSegment.Close()
		p.currentSegment = CreateOrRestoreSegment(p.basePath, p.TopicName, p.ID, *offset)
		p.segments[*offset] = p.currentSegment
	}
	p.memBuf = nil
	return nil
}

func (p *Partition) Restore() error {
	// TODO: list segments inside folder
	dir := path.Join(p.basePath, fmt.Sprintf("%s-%d", p.TopicName, p.ID))
	files, _ := os.ReadDir(dir)
	baseOffsets := make(map[uint64]struct{})
	var currentOffset uint64
	var currentSegment *LogSegment
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fileName := f.Name()
		parts := strings.Split(fileName, ".")
		offsetStr := parts[0]
		offset, _ := strconv.ParseUint(offsetStr, 10, 0)
		if _, exists := baseOffsets[offset]; exists {
			continue
		}
		baseOffsets[offset] = struct{}{}
		// then CreateOrRestoreSegment
		seg := CreateOrRestoreSegment(p.basePath, p.TopicName, p.ID, offset)
		if currentOffset < offset+seg.NumRecords {
			currentOffset = offset + seg.NumRecords
			currentSegment = seg
		}
		p.segments[offset] = seg
	}
	p.offset = currentOffset
	p.currentSegment = currentSegment
	return nil
}

func (p *Partition) Close() {
	p.currentSegment.Close()
}
