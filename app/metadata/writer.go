package metadata

import (
	"fmt"
	"os"
)

func WriteRecordsToLog(topic string, partition int32, records []byte) error {
	logFile, err := os.OpenFile(fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topic, partition), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer logFile.Close()

	_, err = logFile.Write(records)
	return err
}
