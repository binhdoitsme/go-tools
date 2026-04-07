package core

type Message struct {
	TopicName   string
	PartitionID int
	Offset      uint64
	Key         []byte
	Value       []byte
	Timestamp   int64 // Unix millis
	Headers     map[string][]byte
}
