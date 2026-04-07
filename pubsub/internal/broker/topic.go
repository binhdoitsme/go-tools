package broker

import (
	"hash/fnv"
	"sync"

	"github.com/binhdoitsme/pubsub/internal/partition"
)

type Topic struct {
	Name       string
	Partitions []*partition.Partition
	basePath   string
}

func NewTopic(name string, numPartitions int, basePath string) *Topic {
	var partitions []*partition.Partition
	for i := range numPartitions {
		partitions = append(partitions, partition.New(name, i, basePath))
	}
	return &Topic{
		Name:       name,
		Partitions: partitions,
	}
}

func (t *Topic) GetPartition(key []byte) *partition.Partition {
	hasher := fnv.New64()
	hasher.Write(key)
	h := hasher.Sum64()
	return t.Partitions[int(h%uint64(len(t.Partitions)))]
}

func (t *Topic) Flush() {
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, p := range t.Partitions {
		wg.Go(func() { p.Flush() })
	}
}

func (t *Topic) Restore() error {
	for _, p := range t.Partitions {
		err := p.Restore()
		if err != nil {
			return err
		}
	}
	return nil
}
