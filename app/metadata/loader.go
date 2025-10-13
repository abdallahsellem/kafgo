package metadata

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func LoadClusterMetadata() {
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	file, err := os.Open(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read cluster metadata: %v\n", err)
		return
	}
	defer file.Close()

	batchCount := 0
	for {
		// Read record batch using the robust reference implementation
		batch, err := ReadRecordBatch(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Failed to read record batch: %v\n", err)
			continue
		}

		batchCount++
		fmt.Printf("Read batch %d: %d records\n", batchCount, batch.RecordCount)

		// Parse records in the batch
		if err := ParseRecords(batch); err != nil {
			fmt.Printf("Error parsing records in batch %d: %v\n", batchCount, err)
			continue
		}
	}

	fmt.Printf("Total batches read: %d\n", batchCount)
	fmt.Printf("Loaded %d topics from metadata\n", len(TopicsMetadata))
}
func LoadPartitionMetadata(TopicID string, partition int) []byte {
	topicName := ""
	for name, meta := range TopicsMetadata {
		if fmt.Sprintf("%x", meta.TopicID) == TopicID {
			topicName = name
			break
		}
	}
	logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partition)
	buf := make([]byte, 0)
	logData, err := os.ReadFile(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read partition metadata: %v\n", err)
		return buf
	}

	// COMPACT_RECORDS = UVarInt(length + 1) + actual bytes
	recordsLength := len(logData) + 1
	buf = binary.AppendUvarint(buf, uint64(recordsLength))
	buf = append(buf, logData...)

	return buf
}

// ValidateTopicExists checks if a topic exists in the cluster metadata
// by reading the __cluster_metadata topic's log file and looking for TOPIC_RECORD
func ValidateTopicExists(topicName string) bool {
	// Check if topic already loaded in memory
	if _, exists := TopicsMetadata[topicName]; exists {
		return true
	}

	// If not in memory, re-read metadata (in case it was updated)
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	file, err := os.Open(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read cluster metadata for validation: %v\n", err)
		return false
	}
	defer file.Close()

	// Read through batches to find TOPIC_RECORD with matching name
	for {
		batch, err := ReadRecordBatch(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		// Check if this batch contains the topic we're looking for
		if containsTopicRecord(batch, topicName) {
			return true
		}
	}

	return false
}

// ValidatePartitionExists checks if a partition exists for a given topic
// by reading the __cluster_metadata topic's log file and looking for PARTITION_RECORD
func ValidatePartitionExists(topicName string, partitionIndex int32) bool {
	// First check if topic exists
	if !ValidateTopicExists(topicName) {
		return false
	}

	// Check if partition already loaded in memory
	if topicMeta, exists := TopicsMetadata[topicName]; exists {
		for _, partition := range topicMeta.Partitions {
			if partition.PartitionIndex == partitionIndex {
				return true
			}
		}
	}

	// If not in memory, re-read metadata (in case it was updated)
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	file, err := os.Open(logPath)
	if err != nil {
		fmt.Printf("Warning: Could not read cluster metadata for partition validation: %v\n", err)
		return false
	}
	defer file.Close()

	// Get the topic ID for the topic name
	var topicID [16]byte
	if topicMeta, exists := TopicsMetadata[topicName]; exists {
		topicID = topicMeta.TopicID
	} else {
		return false
	}

	// Read through batches to find PARTITION_RECORD with matching topic ID and partition index
	for {
		batch, err := ReadRecordBatch(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		// Check if this batch contains the partition we're looking for
		if containsPartitionRecord(batch, topicID, partitionIndex) {
			return true
		}
	}

	return false
}

// containsTopicRecord checks if a record batch contains a TOPIC_RECORD with the given topic name
func containsTopicRecord(batch *RecordBatch, topicName string) bool {
	data := batch.Records
	offset := 0

	for i := int32(0); i < batch.RecordCount; i++ {
		// Read record length
		recordLen, n := binary.Varint(data[offset:])
		if n <= 0 || offset+n+int(recordLen) > len(data) {
			break
		}
		offset += n

		recordData := data[offset : offset+int(recordLen)]
		offset += int(recordLen)

		// Parse record to check if it's a TOPIC_RECORD with matching name
		if isMatchingTopicRecord(recordData, topicName) {
			return true
		}
	}

	return false
}

// containsPartitionRecord checks if a record batch contains a PARTITION_RECORD with the given topic ID and partition index
func containsPartitionRecord(batch *RecordBatch, topicID [16]byte, partitionIndex int32) bool {
	data := batch.Records
	offset := 0

	for i := int32(0); i < batch.RecordCount; i++ {
		// Read record length
		recordLen, n := binary.Varint(data[offset:])
		if n <= 0 || offset+n+int(recordLen) > len(data) {
			break
		}
		offset += n

		recordData := data[offset : offset+int(recordLen)]
		offset += int(recordLen)

		// Parse record to check if it's a PARTITION_RECORD with matching topic ID and partition index
		if isMatchingPartitionRecord(recordData, topicID, partitionIndex) {
			return true
		}
	}

	return false
}

// isMatchingTopicRecord checks if a record is a TOPIC_RECORD with the given topic name
func isMatchingTopicRecord(recordData []byte, topicName string) bool {
	recordOffset := 0

	// Skip attributes (int8)
	recordOffset += 1

	// Skip timestamp delta (varint)
	if _, n := binary.Varint(recordData[recordOffset:]); n <= 0 {
		return false
	} else {
		recordOffset += n
	}

	// Skip offset delta (varint)
	if _, n := binary.Varint(recordData[recordOffset:]); n <= 0 {
		return false
	} else {
		recordOffset += n
	}

	// Read key length (varint)
	keyLen, n := binary.Varint(recordData[recordOffset:])
	if n <= 0 {
		return false
	}
	recordOffset += n

	// Skip key
	if keyLen == -1 {
		// Null key
	} else if keyLen > 0 {
		if recordOffset+int(keyLen) > len(recordData) {
			return false
		}
		recordOffset += int(keyLen)
	}

	// Read value length (varint)
	valueLen, n := binary.Varint(recordData[recordOffset:])
	if n <= 0 || valueLen <= 0 {
		return false
	}
	recordOffset += n

	// Read value
	if recordOffset+int(valueLen) > len(recordData) {
		return false
	}
	value := recordData[recordOffset : recordOffset+int(valueLen)]

	// Check if this is a TOPIC_RECORD (type 2)
	if len(value) < 2 || value[1] != 2 {
		return false
	}

	// Parse the topic name from the TOPIC_RECORD
	valueOffset := 2 // Skip frame version and record type
	valueOffset++    // Skip TAG_BUFFER

	// Read topic name (COMPACT_STRING)
	nameLen, n := readUvarint(value[valueOffset:])
	if n <= 0 {
		return false
	}
	valueOffset += n
	nameLen-- // Compact string encoding

	if valueOffset+nameLen > len(value) {
		return false
	}

	name := string(value[valueOffset : valueOffset+nameLen])
	return name == topicName
}

// isMatchingPartitionRecord checks if a record is a PARTITION_RECORD with the given topic ID and partition index
func isMatchingPartitionRecord(recordData []byte, topicID [16]byte, partitionIndex int32) bool {
	recordOffset := 0

	// Skip attributes (int8)
	recordOffset += 1

	// Skip timestamp delta (varint)
	if _, n := binary.Varint(recordData[recordOffset:]); n <= 0 {
		return false
	} else {
		recordOffset += n
	}

	// Skip offset delta (varint)
	if _, n := binary.Varint(recordData[recordOffset:]); n <= 0 {
		return false
	} else {
		recordOffset += n
	}

	// Read key length (varint)
	keyLen, n := binary.Varint(recordData[recordOffset:])
	if n <= 0 {
		return false
	}
	recordOffset += n

	// Skip key
	if keyLen == -1 {
		// Null key
	} else if keyLen > 0 {
		if recordOffset+int(keyLen) > len(recordData) {
			return false
		}
		recordOffset += int(keyLen)
	}

	// Read value length (varint)
	valueLen, n := binary.Varint(recordData[recordOffset:])
	if n <= 0 || valueLen <= 0 {
		return false
	}
	recordOffset += n

	// Read value
	if recordOffset+int(valueLen) > len(recordData) {
		return false
	}
	value := recordData[recordOffset : recordOffset+int(valueLen)]

	// Check if this is a PARTITION_RECORD (type 3)
	if len(value) < 2 || value[1] != 3 {
		return false
	}

	// Parse the partition data from the PARTITION_RECORD
	valueOffset := 2 // Skip frame version and record type
	valueOffset++    // Skip TAG_BUFFER

	// Read partition ID (int32)
	if valueOffset+4 > len(value) {
		return false
	}
	readPartitionID := int32(binary.BigEndian.Uint32(value[valueOffset : valueOffset+4]))
	valueOffset += 4

	// Read topic ID (UUID - 16 bytes)
	if valueOffset+16 > len(value) {
		return false
	}
	var readTopicID [16]byte
	copy(readTopicID[:], value[valueOffset:valueOffset+16])

	// Check if both topic ID and partition index match
	return readTopicID == topicID && readPartitionID == partitionIndex
}
