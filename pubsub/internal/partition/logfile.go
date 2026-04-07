package partition

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/binhdoitsme/pubsub/core"
)

const (
	maxSegmentSizeBytes = 1024 * 1024 * 1024 // 1GB
)

type LogSegment struct {
	Topic       string
	PartitionID int
	BaseOffset  uint64 // starts at nextOffset when created
	NumRecords  uint64 // how many messages this segment holds
	Path        string
	File        *os.File
	sizeBytes   uint64
	IsClosed    bool
}

func CreateOrRestoreSegment(basePath, topic string, partitionID int, baseOffset uint64) *LogSegment {
	segment := LogSegment{
		Topic:       topic,
		PartitionID: partitionID,
		BaseOffset:  baseOffset,
		IsClosed:    false,
	}
	filePath := path.Join(basePath, fmt.Sprintf("%s-%d/%016d.log", topic, partitionID, baseOffset))
	segment.Path = filePath
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		return &segment
	}
	segment.File = file

	metaFilePath := strings.ReplaceAll(filePath, ".log", ".meta")
	metaFile, err := os.Open(metaFilePath)
	metaFileInfo, err := metaFile.Stat()
	metadata := make([]byte, metaFileInfo.Size())
	metaFile.Read(metadata)
	recordCount, err := strconv.Atoi(
		strings.ReplaceAll(
			strings.TrimSpace(string(metadata)), ":record-count:", ""))
	segment.NumRecords = uint64(recordCount)

	fileStat, err := file.Stat()
	segment.sizeBytes = uint64(fileStat.Size())
	return &segment
}

func (seg *LogSegment) writeMeta() error {
	content := fmt.Appendf(nil, ":record-count:%d", seg.NumRecords)
	metaFilePath := strings.ReplaceAll(seg.Path, ".log", ".meta")
	return os.WriteFile(metaFilePath, content, 0644)
}

// Write in‑memory messages into the segment file
func (seg *LogSegment) WriteBatch(msgs []core.Message) (*uint64, error) {
	if seg.File == nil {
		os.MkdirAll(path.Dir(seg.Path), 0755)
		file, err := os.OpenFile(seg.Path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		seg.File = file
	}

	for i, msg := range msgs {
		if err := seg.writeRecord(msg); err != nil {
			offset := uint64(i) + seg.BaseOffset
			return &offset, err
		}
	}
	offset := uint64(len(msgs)) + seg.BaseOffset
	return &offset, seg.writeMeta()
}

func (seg *LogSegment) writeRecord(msg core.Message) error {
	// 1. serialize message into a binary blob
	blob, err := seg.encodeMessage(msg)
	if err != nil {
		return err
	}

	contentLength := uint32(len(blob))

	// 2. check if this blob fits in remaining segment space
	if seg.sizeBytes+uint64(contentLength)+4 > maxSegmentSizeBytes {
		return fmt.Errorf("SEGMENT_FULL")
	}

	// 3. write length (4 bytes, big‑endian)
	if err := binary.Write(seg.File, binary.BigEndian, &contentLength); err != nil {
		return err
	}

	// 4. write the binary blob
	_, err = seg.File.Write(blob)
	if err != nil {
		return err
	}

	// 5. update metadata
	seg.NumRecords++
	seg.sizeBytes += uint64(contentLength) + 4 // 4 for the length prefix

	return nil
}

func (seg *LogSegment) encodeMessage(msg core.Message) ([]byte, error) {
	var buf bytes.Buffer

	// 8 bytes: offset
	if err := binary.Write(&buf, binary.BigEndian, msg.Offset); err != nil {
		return nil, err
	}

	// 8 bytes: timestamp (nanoseconds since epoch, as int64)
	if err := binary.Write(&buf, binary.BigEndian, msg.Timestamp); err != nil {
		return nil, err
	}

	headerCount := uint32(len(msg.Headers))
	if err := binary.Write(&buf, binary.BigEndian, headerCount); err != nil {
		return nil, err
	}

	for k, v := range msg.Headers {
		// header key: 4 bytes length + string bytes
		keyLen := uint32(len(k))
		if err := binary.Write(&buf, binary.BigEndian, keyLen); err != nil {
			return nil, err
		}
		buf.WriteString(k)

		// header value: 4 bytes length + bytes
		valLen := uint32(len(v))
		if err := binary.Write(&buf, binary.BigEndian, valLen); err != nil {
			return nil, err
		}
		buf.Write(v)
	}

	// 4 bytes + key
	keyLen := uint32(len(msg.Key))
	if err := binary.Write(&buf, binary.BigEndian, keyLen); err != nil {
		return nil, err
	}
	buf.Write(msg.Key)

	// 4 bytes + value
	valLen := uint32(len(msg.Value))
	if err := binary.Write(&buf, binary.BigEndian, valLen); err != nil {
		return nil, err
	}
	buf.Write(msg.Value)

	return buf.Bytes(), nil
}

type ReadRequest struct {
	StartOffset uint64
	MaxMessages int
}

type ReadResult struct {
	Messages   []core.Message
	NextOffset uint64
}

func (seg *LogSegment) Read(req ReadRequest) (res ReadResult, err error) {
	if seg.File == nil {
		return res, fmt.Errorf("segment file not open")
	}

	// 1. Check if startOffset is in this segment.
	lastOffset := seg.BaseOffset + seg.NumRecords - 1
	if req.StartOffset < seg.BaseOffset || req.StartOffset > lastOffset {
		// Not in this segment.
		return res, nil
	}

	// 2. Seek to start of file (or wherever needed; you may cache index later).
	if _, err := seg.File.Seek(0, io.SeekStart); err != nil {
		return res, err
	}

	var messages []core.Message

	// Use a buffered reader so we can read the length line and then
	// read the next N bytes (the record payload) with io.ReadFull.
	reader := bufio.NewReader(seg.File)

	for {
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return res, err
		}

		// If MaxMessages was specified and we've already collected enough, stop.
		if req.MaxMessages > 0 && len(messages) >= req.MaxMessages {
			break
		}

		content := make([]byte, length)
		if _, err := io.ReadFull(reader, content); err != nil {
			return res, fmt.Errorf("cannot read full record content: %w", err)
		}

		msg, offset, err := seg.parseRecord(content)
		if err != nil {
			return res, fmt.Errorf("cannot parse record: %w", err)
		}

		// If this record is before StartOffset, skip it.
		if offset < req.StartOffset {
			continue
		}

		messages = append(messages, msg)

		// Stop when we've collected the requested number of messages.
		if req.MaxMessages > 0 && len(messages) >= req.MaxMessages {
			break
		}
	}

	nextOffset := seg.BaseOffset + seg.NumRecords
	if len(messages) > 0 {
		nextOffset = messages[len(messages)-1].Offset + 1
	}

	return ReadResult{
		Messages:   messages,
		NextOffset: nextOffset,
	}, nil
}

func (seg *LogSegment) parseRecord(content []byte) (core.Message, uint64, error) {
	var msg core.Message
	var r bytes.Reader
	r.Reset(content)

	var err error

	// 1. Offset: 8 bytes
	var offset uint64
	if err = binary.Read(&r, binary.BigEndian, &offset); err != nil {
		return msg, 0, err
	}
	msg.Offset = offset

	// 2. Timestamp: 8 bytes
	var timestamp int64
	if err = binary.Read(&r, binary.BigEndian, &timestamp); err != nil {
		return msg, 0, err
	}
	msg.Timestamp = timestamp

	// 5. Headers: 4 bytes headerCount, then each header key/value
	var headerCount uint32
	if err = binary.Read(&r, binary.BigEndian, &headerCount); err != nil {
		return msg, 0, err
	}
	if headerCount > 0 {
		msg.Headers = make(map[string][]byte)
	}
	for i := uint32(0); i < headerCount; i++ {
		// header key: 4 bytes length + string bytes
		var keyLen uint32
		if err = binary.Read(&r, binary.BigEndian, &keyLen); err != nil {
			return msg, 0, err
		}
		key := make([]byte, keyLen)
		if _, err = r.Read(key); err != nil {
			return msg, 0, err
		}

		// header value: 4 bytes length + bytes
		var valLen uint32
		if err = binary.Read(&r, binary.BigEndian, &valLen); err != nil {
			return core.Message{}, 0, err
		}
		value := make([]byte, valLen)
		if _, err = r.Read(value); err != nil {
			return core.Message{}, 0, err
		}

		msg.Headers[string(key)] = value
	}

	// 3. Key: 4 bytes length + key bytes
	var keyLen uint32
	if err = binary.Read(&r, binary.BigEndian, &keyLen); err != nil {
		return msg, 0, err
	}
	if keyLen > 0 {
		msg.Key = make([]byte, keyLen)
		if _, err = r.Read(msg.Key); err != nil {
			return msg, 0, err
		}
	}

	// 4. Value: 4 bytes length + value bytes
	var valLen uint32
	if err = binary.Read(&r, binary.BigEndian, &valLen); err != nil {
		return msg, 0, err
	}
	if valLen > 0 {
		msg.Value = make([]byte, valLen)
		if _, err = r.Read(msg.Value); err != nil {
			return msg, 0, err
		}
	}

	// Fill partition and topic (broker layer)
	msg.PartitionID = seg.PartitionID
	msg.TopicName = seg.Topic

	return msg, offset, nil
}

func (seg *LogSegment) Close() error {
	seg.IsClosed = true
	return seg.File.Close()
}
