package metadata

import (
	"encoding/binary"
	"fmt"
)

func ParseRecords(batch *RecordBatch) error {
	data := batch.Records
	offset := 0

	fmt.Printf("Parsing batch with %d records, data len=%d\n", batch.RecordCount, len(data))

	for i := int32(0); i < batch.RecordCount; i++ {
		// Read record length (varint)
		recordLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			fmt.Printf("Failed to read record length at offset %d\n", offset)
			return fmt.Errorf("failed to read record length")
		}
		offset += n

		fmt.Printf("Record %d: length=%d, offset=%d\n", i, recordLen, offset)

		if offset+int(recordLen) > len(data) {
			fmt.Printf("Record length %d exceeds remaining data %d\n", recordLen, len(data)-offset)
			return fmt.Errorf("record length exceeds data")
		}

		recordData := data[offset : offset+int(recordLen)]
		offset += int(recordLen)

		// Parse the record
		if err := ParseRecord(recordData); err != nil {
			fmt.Printf("Error parsing record %d: %v\n", i, err)
			continue
		}
	}

	return nil
}

func ParseRecord(data []byte) error {
	offset := 0

	fmt.Printf("Parsing record, data len=%d, hex=%x\n", len(data), data[:min(len(data), 20)])

	// Read attributes (int8)
	offset += 1

	// Read timestamp delta (varint)
	_, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read timestamp delta")
	}
	offset += n

	// Read offset delta (varint)
	_, n = binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read offset delta")
	}
	offset += n

	// Read key length (varint)
	keyLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read key length")
	}
	offset += n

	fmt.Printf("Key length: %d, offset now: %d\n", keyLen, offset)

	// Read key (keyLen can be -1 for null)
	var key []byte
	if keyLen == -1 {
		// Null key
		fmt.Printf("Key is null\n")
	} else if keyLen > 0 {
		if offset+int(keyLen) > len(data) {
			return fmt.Errorf("key length exceeds data")
		}
		key = data[offset : offset+int(keyLen)]
		offset += int(keyLen)
		fmt.Printf("Key: %x\n", key)
	}

	// Read value length (varint)
	valueLen, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read value length")
	}
	offset += n

	fmt.Printf("Value length: %d\n", valueLen)

	// Read value
	var value []byte
	if valueLen == -1 {
		// Null value
		fmt.Printf("Value is null\n")
	} else if valueLen > 0 {
		if offset+int(valueLen) > len(data) {
			return fmt.Errorf("value length exceeds data")
		}
		value = data[offset : offset+int(valueLen)]
		offset += int(valueLen)
	}

	// Read headers count (varint)
	headersCount, n := binary.Varint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read headers count")
	}
	offset += n

	fmt.Printf("Headers count: %d\n", headersCount)

	// Skip headers for now
	for i := int64(0); i < headersCount; i++ {
		// Read header key length
		headerKeyLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to read header key length")
		}
		offset += n
		offset += int(headerKeyLen) // Skip header key

		// Read header value length
		headerValueLen, n := binary.Varint(data[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to read header value length")
		}
		offset += n
		offset += int(headerValueLen) // Skip header value
	}

	// Parse the actual record from the value
	if value != nil && len(value) > 0 {
		recordType := parseRecordTypeFromValue(value)
		fmt.Printf("Record type from value: %d\n", recordType)

		// Parse based on record type
		switch recordType {
		case 2: // TopicRecord
			fmt.Printf("Parsing TopicRecord...\n")
			if len(value) > 2 {
				ParseTopicRecordFromValue(value[2:])
			}
		case 3: // PartitionRecord
			fmt.Printf("Parsing PartitionRecord...\n")
			if len(value) > 2 {
				ParsePartitionRecordFromValue(value[2:])
			}
		default:
			fmt.Printf("Unknown or unsupported record type: %d\n", recordType)
		}
	}

	return nil
}

func parseRecordTypeFromValue(value []byte) int8 {
	if len(value) >= 2 {
		// First byte is the frame version, second byte is the record type
		return int8(value[1])
	}
	return -1
}

func ParseTopicRecordFromValue(data []byte) error {
	offset := 0

	fmt.Printf("parseTopicRecord: data len=%d, hex=%x\n", len(data), data)

	// Skip TAG_BUFFER at the beginning (for flexible versions)
	if offset < len(data) {
		offset++ // Skip TAG_BUFFER byte
	}

	// Read topic name (COMPACT_STRING - uses unsigned varint)
	nameLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read name length")
	}
	offset += n
	nameLen-- // Compact string encoding: length = N + 1

	fmt.Printf("Topic name length: %d (after decoding), offset: %d\n", nameLen, offset)

	if offset+nameLen > len(data) {
		return fmt.Errorf("name length %d exceeds data: offset=%d, data len=%d", nameLen, offset, len(data))
	}
	name := string(data[offset : offset+nameLen])
	offset += nameLen

	fmt.Printf("Topic name: %s, offset now: %d\n", name, offset)

	// Read topic ID (UUID - 16 bytes)
	if offset+16 > len(data) {
		return fmt.Errorf("not enough data for topic ID: need %d, have %d", offset+16, len(data))
	}
	var topicID [16]byte
	copy(topicID[:], data[offset:offset+16])

	// Store the topic metadata
	TopicsMetadata[name] = &TopicMetadata{
		Name:       name,
		TopicID:    topicID,
		Partitions: []PartitionMetadata{},
	}

	fmt.Printf("Parsed TopicRecord: name=%s, topicID=%x\n", name, topicID)
	return nil
}

func ParsePartitionRecordFromValue(data []byte) error {
	offset := 0

	fmt.Printf("parsePartitionRecord: data len=%d, hex=%x\n", len(data), data[:min(len(data), 40)])

	// Skip TAG_BUFFER at the beginning (for flexible versions)
	if offset < len(data) {
		offset++ // Skip TAG_BUFFER byte
	}

	// Read partition ID (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for partition ID")
	}
	partitionID := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	fmt.Printf("Partition ID: %d\n", partitionID)

	// Read topic ID (UUID - 16 bytes)
	if offset+16 > len(data) {
		return fmt.Errorf("not enough data for topic ID")
	}
	var topicID [16]byte
	copy(topicID[:], data[offset:offset+16])
	offset += 16

	// Read replicas (COMPACT_ARRAY - uses unsigned varint)
	replicasLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read replicas length")
	}
	offset += n
	replicasLen-- // Compact array encoding: length = N + 1

	replicas := make([]int32, replicasLen)
	for i := 0; i < replicasLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for replica")
		}
		replicas[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Read ISR (COMPACT_ARRAY - uses unsigned varint)
	isrLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read ISR length")
	}
	offset += n
	isrLen-- // Compact array encoding: length = N + 1

	isr := make([]int32, isrLen)
	for i := 0; i < isrLen; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("not enough data for ISR")
		}
		isr[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}

	// Skip removing replicas (COMPACT_ARRAY - uses unsigned varint)
	removingLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read removing replicas length")
	}
	offset += n
	removingLen-- // Compact array encoding: length = N + 1
	offset += removingLen * 4

	// Skip adding replicas (COMPACT_ARRAY - uses unsigned varint)
	addingLen, n := readUvarint(data[offset:])
	if n <= 0 {
		return fmt.Errorf("failed to read adding replicas length")
	}
	offset += n
	addingLen-- // Compact array encoding: length = N + 1
	offset += addingLen * 4

	// Read leader (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for leader")
	}
	leader := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Skip leader recovery state (int8)
	offset += 1

	// Read leader epoch (int32)
	if offset+4 > len(data) {
		return fmt.Errorf("not enough data for leader epoch")
	}
	// Find the topic and add partition
	for _, topic := range TopicsMetadata {
		if topic.TopicID == topicID {
			topic.Partitions = append(topic.Partitions, PartitionMetadata{
				PartitionIndex: partitionID,
				LeaderID:       leader,
				ReplicaNodes:   replicas,
				IsrNodes:       isr,
			})
			fmt.Printf("Added partition %d to topic %s (leader: %d)\n", partitionID, topic.Name, leader)
			break
		}
	}

	return nil
}

func readUvarint(data []byte) (int, int) {
	v, n := binary.Uvarint(data)
	return int(v), n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
