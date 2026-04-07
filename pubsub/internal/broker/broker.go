package broker

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/binhdoitsme/pubsub/core"
)

const (
	numPartitions int = 4
)

type Broker struct {
	lock     sync.RWMutex
	topics   map[string]*Topic
	basePath string
}

func NewBroker(basePath string) *Broker {
	broker := &Broker{
		topics:   make(map[string]*Topic),
		basePath: basePath,
	}
	broker.restore()
	return broker
}

func (b *Broker) NewTopic(name string, numPartitions int) *Topic {
	if topic, exists := b.topics[name]; exists {
		return topic
	}
	topic := NewTopic(name, numPartitions, b.basePath)
	b.topics[name] = topic
	return topic
}

func (b *Broker) restore() {
	b.lock.Lock()
	defer b.lock.Unlock()
	// TODO: restore broker state from disk
	// 1. list topics
	topicFolders, _ := os.ReadDir(b.basePath)
	// 2. restore each
	topics := make(map[string]int)
	for _, topicFolder := range topicFolders {
		if !topicFolder.IsDir() {
			continue
		}
		folderName := topicFolder.Name()
		parts := strings.Split(folderName, "-")
		topicName := parts[0]
		partitionID, _ := strconv.Atoi(parts[1])
		if _, exists := topics[topicName]; !exists {
			topics[topicName] = partitionID
		}
		if topics[topicName] < partitionID {
			topics[topicName] = partitionID // max partitionID
		}
	}
	for topicName, maxPartitionID := range topics {
		topic := NewTopic(topicName, maxPartitionID+1, b.basePath)
		topic.Restore()
		b.topics[topicName] = topic
	}
}

func (b *Broker) Commit(msg *core.Message) (*core.Message, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	topic := msg.TopicName
	if _, exists := b.topics[topic]; !exists {
		b.topics[topic] = NewTopic(topic, numPartitions, b.basePath)
	}

	partition := b.topics[topic].GetPartition(msg.Key)
	_, err := partition.Append(msg)
	return msg, err
}

func (b *Broker) Fetch(topic string, partitionID int, fromOffset uint64, toOffset ...uint64) []core.Message {
	if _, exists := b.topics[topic]; !exists {
		return []core.Message{}
	}
	var valToOffset uint64
	partition := b.topics[topic].Partitions[partitionID]
	if len(toOffset) > 0 {
		valToOffset = toOffset[0]
	} else {
		valToOffset = uint64(partition.Size() - 1)
	}
	return partition.Read(fromOffset, valToOffset-fromOffset+1)
}

func (b *Broker) Flush() {
	b.lock.Lock()
	defer b.lock.Unlock()
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, topic := range b.topics {
		wg.Go(func() { topic.Flush() })
	}
}
