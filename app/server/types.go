package server

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID uint32
	ClientID      reqClientID
}
type ResponseHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID uint32
}

type ApiKeyInfo struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}
type reqClientID struct {
	length  uint16
	content string
}

type DescribeTopicPartitionsRequest struct {
	ClientID   string
	TopicNames []string
}

var SupportedApiKeys = []ApiKeyInfo{
	{Key: 0, MinVersion: 0, MaxVersion: 11}, // Produce - FIXED: was duplicate Fetch
	{Key: 1, MinVersion: 0, MaxVersion: 16}, // Fetch
	{Key: 18, MinVersion: 0, MaxVersion: 4}, // ApiVersions
	{Key: 75, MinVersion: 0, MaxVersion: 0}, // DescribeTopicPartitions
}

type FetchRequest struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchTopic
	ForgottenTopicsData []ForgottenTopic
	RackID              string
}

type FetchTopic struct {
	TopicID    [16]byte
	Partitions []FetchPartition
}

type FetchPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

type ForgottenTopic struct {
	TopicID    [16]byte
	Partitions []int32
}

type ProduceRequest struct {
	TransactionalID string
	Acks            int16
	TimeoutMs       int32
	TopicData       []ProduceTopic
	TaggedFields    []byte
}

// ProduceTopic represents topic data in produce request
type ProduceTopic struct {
	Name          string
	PartitionData []ProducePartition
	TaggedFields  []byte
}

// ProducePartition represents partition data in produce request
// This contains the actual records to produce
type ProducePartition struct {
	Index        int32
	Records      []byte // COMPACT_RECORDS (raw bytes)
	TaggedFields []byte
}

// ProduceResponse types
type ProduceResponse struct {
	TopicResponses []ProduceTopicResponse
	ThrottleTimeMs int32
	TaggedFields   []byte
}

// ProduceTopicResponse represents topic response in produce response
type ProduceTopicResponse struct {
	Name               string
	PartitionResponses []ProducePartitionResponse
	TaggedFields       []byte
}

// ProducePartitionResponse represents partition response in produce response
type ProducePartitionResponse struct {
	PartitionID    int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
	RecordErrors   []RecordError
	ErrorMessage   string
	TaggedFields   []byte
}

// RecordError represents individual record errors
type RecordError struct {
	BatchIndex int32
	BatchError string
}
