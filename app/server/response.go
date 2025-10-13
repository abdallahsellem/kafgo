package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"sort"

	"kafgo/app/metadata"
)

// Kafka protocol error codes (subset)
const (
	ErrNone                    int16 = 0
	UNKNOWN_TOPIC_ID           int16 = 100
	UNKNOWN_TOPIC_OR_PARTITION int16 = 3
)

func BuildDescribeTopicPartitionsResponse(request DescribeTopicPartitionsRequest) []byte {
	response := make([]byte, 0)
	topicsMetadata := metadata.GetTopicMetadata()

	// Tag Buffer
	response = append(response, 0x00)
	// Throttle Time (INT32)
	response = AppendInt32(response, 0)

	// If no specific topics requested, return all topics in alphabetical order
	topicNames := request.TopicNames
	if len(topicNames) == 0 {
		// Get all topic names and sort them alphabetically
		allTopicNames := make([]string, 0, len(topicsMetadata))
		for name := range topicsMetadata {
			allTopicNames = append(allTopicNames, name)
		}
		sort.Strings(allTopicNames)
		topicNames = allTopicNames
	} else {
		// Sort the requested topic names alphabetically
		sort.Strings(topicNames)
	}

	// Topics Array (COMPACT_ARRAY)
	numTopics := len(topicNames)
	response = append(response, byte(numTopics+1))

	for _, topicName := range topicNames {
		topic, exists := topicsMetadata[topicName]

		if !exists {
			// Topic not found
			response = AppendInt16(response, 3) // UNKNOWN_TOPIC_OR_PARTITION
			response = append(response, byte(len(topicName)+1))
			response = append(response, []byte(topicName)...)
			response = append(response, make([]byte, 16)...) // Empty UUID
			response = append(response, 0x00)                // is_internal = false
			response = append(response, 0x01)                // 0 partitions
			response = AppendInt32(response, -2147483648)
			response = append(response, 0x00) // TAG_BUFFER
		} else {
			// Topic found
			response = AppendInt16(response, 0) // No error

			// Topic Name (COMPACT_STRING)
			response = append(response, byte(len(topicName)+1))
			response = append(response, []byte(topicName)...)

			// Topic ID (UUID)
			response = append(response, topic.TopicID[:]...)

			// Is Internal (BOOLEAN)
			response = append(response, 0x00) // false

			// Partitions Array (COMPACT_ARRAY)
			// Sort partitions by partition index
			sort.Slice(topic.Partitions, func(i, j int) bool {
				return topic.Partitions[i].PartitionIndex < topic.Partitions[j].PartitionIndex
			})

			response = append(response, byte(len(topic.Partitions)+1))

			for _, partition := range topic.Partitions {
				// Error Code
				response = AppendInt16(response, 0) // No error

				// Partition Index
				response = AppendInt32(response, partition.PartitionIndex)

				// Leader ID
				response = AppendInt32(response, partition.LeaderID)

				// Leader Epoch
				response = AppendInt32(response, -1)

				// Replica Nodes (COMPACT_ARRAY)
				response = append(response, byte(len(partition.ReplicaNodes)+1))
				for _, replica := range partition.ReplicaNodes {
					response = AppendInt32(response, replica)
				}

				// ISR Nodes (COMPACT_ARRAY)
				response = append(response, byte(len(partition.IsrNodes)+1))
				for _, isr := range partition.IsrNodes {
					response = AppendInt32(response, isr)
				}

				// Eligible Leader Replicas (COMPACT_ARRAY - empty)
				response = append(response, 0x01)

				// Last Known ELR (COMPACT_ARRAY - empty)
				response = append(response, 0x01)

				// Offline Replicas (COMPACT_ARRAY - empty)
				response = append(response, 0x01)

				// TAG_BUFFER
				response = append(response, 0x00)
			}

			// Topic Authorized Operations
			response = AppendInt32(response, -2147483648)

			// TAG_BUFFER
			response = append(response, 0x00)
		}
	}

	// Next Cursor (null = -1)
	response = append(response, 0xFF)

	// Final TAG_BUFFER
	response = append(response, 0x00)

	fmt.Printf("Sent DescribeTopicPartitions response (topics=%d, body_len=%d)\n",
		numTopics, len(response))

	return response
}

func WriteResponse(conn net.Conn, correlationID uint32, body []byte) error {
	response := make([]byte, 0, 8+len(body))

	// message_size
	response = AppendInt32(response, int32(4+len(body)))

	// correlation_id
	corrBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(corrBuf, correlationID)
	response = append(response, corrBuf...)

	// body
	response = append(response, body...)

	_, err := conn.Write(response)
	return err
}

func BuildErrorResponse(errorCode int16) []byte {
	response := make([]byte, 0)
	response = AppendInt16(response, errorCode)
	return response
}

func AppendInt16(buf []byte, val int16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(val))
	return append(buf, b...)
}

func AppendInt32(buf []byte, val int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(val))
	return append(buf, b...)
}

// Helper function to append INT64
func AppendInt64(buf []byte, val int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	return append(buf, b...)
}
func BuildFetchResponse(header ResponseHeader, req FetchRequest, buf []byte) []byte {
	// Build a quick lookup from TopicID -> TopicMetadata
	topicsByID := make(map[[16]byte]*metadata.TopicMetadata)
	for _, t := range metadata.GetTopicMetadata() {
		topicsByID[t.TopicID] = t
	}

	// Responses (COMPACT_ARRAY) - match the topics from request
	buf = append(buf, byte(len(req.Topics)+1)) // Compact array length
	fmt.Printf("Response Bytes from build before compact : % X\n", buf)

	// For each topic in the request, build a response
	for _, topicReq := range req.Topics {
		// TopicID (UUID - 16 bytes) - same as request
		buf = append(buf, topicReq.TopicID[:]...)

		// Locate topic by ID
		topicMeta, topicExists := topicsByID[topicReq.TopicID]

		// Partitions (COMPACT_ARRAY) - match partitions from request
		buf = append(buf, byte(len(topicReq.Partitions)+1)) // Compact array length

		// Build a quick lookup of valid partitions if topic exists
		validPartitions := make(map[int32]struct{})
		if topicExists {
			for _, p := range topicMeta.Partitions {
				validPartitions[p.PartitionIndex] = struct{}{}
			}
		}

		// For each partition in the request, build a partition response
		for _, partReq := range topicReq.Partitions {
			// PartitionIndex (INT32) - same as request
			buf = AppendInt32(buf, partReq.Partition)

			// ErrorCode (INT16)
			errCode := ErrNone
			if !topicExists {
				errCode = UNKNOWN_TOPIC_ID
			} else {
				if _, ok := validPartitions[partReq.Partition]; !ok {
					errCode = UNKNOWN_TOPIC_ID
				}
			}
			buf = AppendInt16(buf, errCode)

			// HighWatermark (INT64) = fetch_offset (for now)
			buf = AppendInt64(buf, partReq.FetchOffset)

			// LastStableOffset (INT64) = fetch_offset (for now)
			buf = AppendInt64(buf, partReq.FetchOffset)

			// LogStartOffset (INT64) = from request
			buf = AppendInt64(buf, partReq.LogStartOffset)

			// AbortedTransactions (COMPACT_ARRAY) - empty
			buf = append(buf, 0x01) // Length = 1 (0 elements)

			// PreferredReadReplica (INT32) = -1 (no preference)
			buf = AppendInt32(buf, -1)

			if errCode == ErrNone {
				buf = append(buf, metadata.LoadPartitionMetadata(fmt.Sprintf("%x", topicReq.TopicID), int(partReq.Partition))...)
			} else {
				// If error, write empty record set
				buf = append(buf, 0x00)
			}

			// TAG_BUFFER for partition (flexible version)
			buf = append(buf, uint8(0x00))
		}
	}

	// Final TAG_BUFFER for responses array (flexible version)
	buf = append(buf, uint8(0x00))
	buf = append(buf, uint8(0x00))
	fmt.Printf("Sent Fetch response with %d topics, body size=%d\n", len(req.Topics), len(buf))
	return buf
}

func BuildProduceResponse(header RequestHeader, req ProduceRequest) []byte {
	response := make([]byte, 0)
	// TAG_BUFFER for main response
	response = append(response, 0x00)
	// TopicResponses (COMPACT_ARRAY)
	response = append(response, byte(len(req.TopicData)+1))

	for _, topicReq := range req.TopicData {
		// Topic Name (COMPACT_STRING)
		response = append(response, byte(len(topicReq.Name)+1))
		response = append(response, []byte(topicReq.Name)...)

		// PartitionResponses (COMPACT_ARRAY)
		response = append(response, byte(len(topicReq.PartitionData)+1))

		topicExists := metadata.ValidateTopicExists(topicReq.Name)

		for _, partReq := range topicReq.PartitionData {
			// Partition ID (INT32)
			response = AppendInt32(response, partReq.Index)

			errorCode := int16(0)
			baseOffset := int64(0)
			logStartOffset := int64(0)
			if !topicExists {
				errorCode = UNKNOWN_TOPIC_OR_PARTITION
				baseOffset = -1
				logStartOffset = -1
			} else if !metadata.ValidatePartitionExists(topicReq.Name, partReq.Index) {
				errorCode = UNKNOWN_TOPIC_OR_PARTITION
				baseOffset = -1
				logStartOffset = -1
			}

			// ErrorCode (INT16)
			response = AppendInt16(response, errorCode)
			// BaseOffset (INT64)
			response = AppendInt64(response, baseOffset)
			// LogAppendTime (INT64)
			response = AppendInt64(response, -1)
			// LogStartOffset (INT64)
			response = AppendInt64(response, logStartOffset)
			// RecordErrors (COMPACT_ARRAY)
			response = append(response, 0x01)
			// ErrorMessage (COMPACT_STRING)
			response = append(response, 0x00)
			// TAG_BUFFER for partition response
			response = append(response, 0x00)

			if errorCode == 0 {
				fmt.Printf("Produce to topic %s partition %d succeeded\n", topicReq.Name, partReq.Index)
				if err := metadata.WriteRecordsToLog(topicReq.Name, partReq.Index, partReq.Records); err != nil {
					fmt.Printf("Failed to write records to log: %v\n", err)
				}
			} else {
				fmt.Printf("Produce to topic %s partition %d failed with error code %d\n", topicReq.Name, partReq.Index, errorCode)
			}
		}
		// TAG_BUFFER for topic response
		response = append(response, 0x00)
	}
	// ThrottleTimeMs (INT32)
	response = AppendInt32(response, 0)
	// TAG_BUFFER for main response
	response = append(response, 0x00)

	fmt.Printf("Built Produce response: %d topics, body size=%d\n", len(req.TopicData), len(response))
	return response

}
