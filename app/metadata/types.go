package metadata

type TopicMetadata struct {
	Name       string
	TopicID    [16]byte
	Partitions []PartitionMetadata
}

type PartitionMetadata struct {
	PartitionIndex int32
	LeaderID       int32
	ReplicaNodes   []int32
	IsrNodes       []int32
}

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordCount          int32
	Records              []byte
}

var TopicsMetadata = make(map[string]*TopicMetadata)

func GetTopicMetadata() map[string]*TopicMetadata {
	return TopicsMetadata
}
