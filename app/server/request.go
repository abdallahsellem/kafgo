package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func ReadRequest(conn net.Conn) (RequestHeader, []byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return RequestHeader{}, nil, err
	}
	messageSize := binary.BigEndian.Uint32(sizeBuf)

	messageBuf := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, messageBuf); err != nil {
		return RequestHeader{}, nil, err
	}

	if len(messageBuf) < 8 {
		return RequestHeader{}, nil, fmt.Errorf("incomplete header")
	}

	// ClientID (COMPACT_STRING)
	reqClientID := reqClientID{}
	reqClientID.length = binary.BigEndian.Uint16(messageBuf[8:10])
	if reqClientID.length > 0 {
		reqClientID.content = string(messageBuf[10 : 10+reqClientID.length])
	}
	newOffset := 8 + 2 + reqClientID.length + 1 // 1 for TAG_BUFFER
	header := RequestHeader{
		ApiKey:        int16(binary.BigEndian.Uint16(messageBuf[0:2])),
		ApiVersion:    int16(binary.BigEndian.Uint16(messageBuf[2:4])),
		CorrelationID: binary.BigEndian.Uint32(messageBuf[4:8]),
		ClientID:      reqClientID,
	}

	return header, messageBuf[newOffset:], nil
}

func ParseDescribeTopicPartitionsRequest(body []byte) DescribeTopicPartitionsRequest {
	var offset int

	// Topics (COMPACT_ARRAY)
	numTopics := int(body[offset]) - 1
	offset++

	topicNames := make([]string, 0, numTopics)
	for i := 0; i < numTopics; i++ {
		// Topic Name (COMPACT_STRING)
		topicLength := int(body[offset]) - 1
		offset++
		topicName := string(body[offset : offset+topicLength])
		offset += topicLength
		offset++ // TAG_BUFFER
		topicNames = append(topicNames, topicName)
	}

	return DescribeTopicPartitionsRequest{
		TopicNames: topicNames,
	}
}

func ParseFetchRequest(body []byte) FetchRequest {
	var req FetchRequest
	offset := 0

	// MaxWaitMs (INT32)
	req.MaxWaitMs = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// MinBytes (INT32)
	req.MinBytes = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// MaxBytes (INT32)
	req.MaxBytes = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// IsolationLevel (INT8)
	req.IsolationLevel = int8(body[offset])
	offset += 1

	// SessionID (INT32)
	req.SessionID = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// SessionEpoch (INT32)
	req.SessionEpoch = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// Topics (COMPACT_ARRAY)
	topicsCompactLen, n := binary.Uvarint(body[offset:])
	offset += n
	req.Topics = make([]FetchTopic, 0)

	if topicsCompactLen > 0 {
		topicsLen := int(topicsCompactLen) - 1
		req.Topics = make([]FetchTopic, topicsLen)

		for i := 0; i < topicsLen; i++ {
			var topic FetchTopic

			// TopicID (UUID - 16 bytes)
			copy(topic.TopicID[:], body[offset:offset+16])
			offset += 16

			// Partitions (COMPACT_ARRAY)
			partitionsCompactLen, n := binary.Uvarint(body[offset:])
			offset += n
			topic.Partitions = make([]FetchPartition, 0)

			if partitionsCompactLen > 0 {
				partitionsLen := int(partitionsCompactLen) - 1
				topic.Partitions = make([]FetchPartition, partitionsLen)

				for j := 0; j < partitionsLen; j++ {
					var partition FetchPartition

					// Partition (INT32)
					partition.Partition = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4

					// CurrentLeaderEpoch (INT32)
					partition.CurrentLeaderEpoch = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4

					// FetchOffset (INT64)
					partition.FetchOffset = int64(binary.BigEndian.Uint64(body[offset : offset+8]))
					offset += 8

					// LastFetchedEpoch (INT32)
					partition.LastFetchedEpoch = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4

					// LogStartOffset (INT64)
					partition.LogStartOffset = int64(binary.BigEndian.Uint64(body[offset : offset+8]))
					offset += 8

					// PartitionMaxBytes (INT32)
					partition.PartitionMaxBytes = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4

					// TAG_BUFFER for partition
					tagBufferLen, n := binary.Uvarint(body[offset:])
					offset += n
					offset += int(tagBufferLen)

					topic.Partitions[j] = partition
				}
			}

			// TAG_BUFFER for topic
			tagBufferLen, n := binary.Uvarint(body[offset:])
			offset += n
			offset += int(tagBufferLen)

			req.Topics[i] = topic
		}
	}

	// ForgottenTopicsData (COMPACT_ARRAY)
	forgottenTopicsCompactLen, n := binary.Uvarint(body[offset:])
	offset += n
	req.ForgottenTopicsData = make([]ForgottenTopic, 0)

	if forgottenTopicsCompactLen > 0 {
		forgottenTopicsLen := int(forgottenTopicsCompactLen) - 1
		req.ForgottenTopicsData = make([]ForgottenTopic, forgottenTopicsLen)

		for i := 0; i < forgottenTopicsLen; i++ {
			var forgottenTopic ForgottenTopic

			// TopicID (UUID - 16 bytes)
			copy(forgottenTopic.TopicID[:], body[offset:offset+16])
			offset += 16

			// Partitions (COMPACT_ARRAY)
			partitionsCompactLen, n := binary.Uvarint(body[offset:])
			offset += n

			if partitionsCompactLen > 0 {
				partitionsLen := int(partitionsCompactLen) - 1
				forgottenTopic.Partitions = make([]int32, partitionsLen)

				for j := 0; j < partitionsLen; j++ {
					// Partition (INT32)
					forgottenTopic.Partitions[j] = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4
				}
			}

			// TAG_BUFFER for forgotten topic
			tagBufferLen, n := binary.Uvarint(body[offset:])
			offset += n
			offset += int(tagBufferLen)

			req.ForgottenTopicsData[i] = forgottenTopic
		}
	}

	// RackID (COMPACT_STRING)
	rackIDCompactLen, n := binary.Uvarint(body[offset:])
	offset += n

	if rackIDCompactLen > 0 {
		rackIDLen := int(rackIDCompactLen) - 1
		req.RackID = string(body[offset : offset+rackIDLen])
		offset += rackIDLen
	}

	// TAG_BUFFER for main request
	tagBufferLen, n := binary.Uvarint(body[offset:])
	offset += n
	offset += int(tagBufferLen)

	return req
}

func ParseProduceRequest(body []byte) ProduceRequest {
	var req ProduceRequest
	offset := 0

	// TransactionalID (COMPACT_STRING)
	txnIDCompactLen, n := binary.Uvarint(body[offset:])
	offset += n

	if txnIDCompactLen > 0 {
		txnIDLen := int(txnIDCompactLen) - 1
		req.TransactionalID = string(body[offset : offset+txnIDLen])
		offset += txnIDLen
	}

	// Acks (INT16)
	req.Acks = int16(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2

	// TimeoutMs (INT32)
	req.TimeoutMs = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4

	// TopicData (COMPACT_ARRAY)
	topicDataCompactLen, n := binary.Uvarint(body[offset:])
	offset += n
	req.TopicData = make([]ProduceTopic, 0)

	if topicDataCompactLen > 0 {
		topicDataLen := int(topicDataCompactLen) - 1
		req.TopicData = make([]ProduceTopic, topicDataLen)

		for i := 0; i < topicDataLen; i++ {
			var topic ProduceTopic

			// Topic Name (COMPACT_STRING)
			topicNameCompactLen, n := binary.Uvarint(body[offset:])
			offset += n

			if topicNameCompactLen > 0 {
				topicNameLen := int(topicNameCompactLen) - 1
				topic.Name = string(body[offset : offset+topicNameLen])
				offset += topicNameLen
			}

			// PartitionData (COMPACT_ARRAY)
			partitionDataCompactLen, n := binary.Uvarint(body[offset:])
			offset += n
			topic.PartitionData = make([]ProducePartition, 0)

			if partitionDataCompactLen > 0 {
				partitionDataLen := int(partitionDataCompactLen) - 1
				topic.PartitionData = make([]ProducePartition, partitionDataLen)

				for j := 0; j < partitionDataLen; j++ {
					var partition ProducePartition

					// Index (INT32)
					partition.Index = int32(binary.BigEndian.Uint32(body[offset : offset+4]))
					offset += 4

					// Records (COMPACT_RECORDS)
					recordsCompactLen, n := binary.Uvarint(body[offset:])
					offset += n

					if recordsCompactLen > 0 {
						recordsLen := int(recordsCompactLen) - 1
						partition.Records = make([]byte, recordsLen)
						copy(partition.Records, body[offset:offset+recordsLen])
						offset += recordsLen
					}

					// TaggedFields (TAG_BUFFER)
					tagBufferLen, n := binary.Uvarint(body[offset:])
					offset += n
					partition.TaggedFields = make([]byte, tagBufferLen)
					copy(partition.TaggedFields, body[offset:offset+int(tagBufferLen)])
					offset += int(tagBufferLen)

					topic.PartitionData[j] = partition
				}
			}

			// TaggedFields (TAG_BUFFER) for topic
			tagBufferLen, n := binary.Uvarint(body[offset:])
			offset += n
			topic.TaggedFields = make([]byte, tagBufferLen)
			copy(topic.TaggedFields, body[offset:offset+int(tagBufferLen)])
			offset += int(tagBufferLen)

			req.TopicData[i] = topic
		}
	}

	// TaggedFields (TAG_BUFFER) for main request
	tagBufferLen, n := binary.Uvarint(body[offset:])
	offset += n
	req.TaggedFields = make([]byte, tagBufferLen)
	copy(req.TaggedFields, body[offset:offset+int(tagBufferLen)])
	offset += int(tagBufferLen)

	return req
}
