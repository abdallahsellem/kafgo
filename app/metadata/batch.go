package metadata

import (
	"encoding/binary"
	"io"
)

func ReadRecordBatch(r io.Reader) (*RecordBatch, error) {
	batch := &RecordBatch{}

	// Read base offset (8 bytes)
	if err := binary.Read(r, binary.BigEndian, &batch.BaseOffset); err != nil {
		return nil, err
	}

	// Read batch length (4 bytes)
	if err := binary.Read(r, binary.BigEndian, &batch.BatchLength); err != nil {
		return nil, err
	}

	// Read the rest of the batch header and records
	batchData := make([]byte, batch.BatchLength)
	if _, err := io.ReadFull(r, batchData); err != nil {
		return nil, err
	}

	offset := 0

	// Parse batch header
	batch.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.Magic = int8(batchData[offset])
	offset += 1

	batch.CRC = binary.BigEndian.Uint32(batchData[offset : offset+4])
	offset += 4

	batch.Attributes = int16(binary.BigEndian.Uint16(batchData[offset : offset+2]))
	offset += 2

	batch.LastOffsetDelta = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.BaseTimestamp = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.ProducerID = int64(binary.BigEndian.Uint64(batchData[offset : offset+8]))
	offset += 8

	batch.ProducerEpoch = int16(binary.BigEndian.Uint16(batchData[offset : offset+2]))
	offset += 2

	batch.BaseSequence = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	batch.RecordCount = int32(binary.BigEndian.Uint32(batchData[offset : offset+4]))
	offset += 4

	// Store remaining data as records
	batch.Records = batchData[offset:]

	return batch, nil
}
