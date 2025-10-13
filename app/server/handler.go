package server

import (
	"fmt"
	"io"
	"net"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Connection established from", conn.RemoteAddr())

	for {
		header, body, err := ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading request:", err)
			}
			return
		}

		response := HandleRequest(header, body)

		if err := WriteResponse(conn, header.CorrelationID, response); err != nil {
			fmt.Println("Error writing response:", err)
			return
		}
	}
}

func HandleRequest(header RequestHeader, body []byte) []byte {
	switch header.ApiKey {
	case 0:
		return HandleProduce(header, body)
	case 18:
		return HandleApiVersions(header, body)
	case 75:
		return HandleDescribeTopicPartitions(header, body)
	case 1:
		return HandleFetch(header, body)
	default:
		fmt.Printf("Unknown API key: %d\n", header.ApiKey)
		return BuildErrorResponse(35)
	}
}

func HandleApiVersions(header RequestHeader, body []byte) []byte {
	fmt.Printf("Received ApiVersions request (version=%d, correlation_id=%d)\n",
		header.ApiVersion, header.CorrelationID)

	var errorCode int16 = 0
	if header.ApiVersion > 4 {
		errorCode = 35
	}

	response := make([]byte, 0)
	response = AppendInt16(response, errorCode)
	response = append(response, byte(len(SupportedApiKeys)+1))

	for _, apiKey := range SupportedApiKeys {
		response = AppendInt16(response, apiKey.Key)
		response = AppendInt16(response, apiKey.MinVersion)
		response = AppendInt16(response, apiKey.MaxVersion)
		response = append(response, 0x00)
	}

	response = AppendInt32(response, 0)
	response = append(response, 0x00)

	return response
}

func HandleDescribeTopicPartitions(header RequestHeader, body []byte) []byte {
	fmt.Printf("Received DescribeTopicPartitions request (version=%d, correlation_id=%d)\n",
		header.ApiVersion, header.CorrelationID)

	request := ParseDescribeTopicPartitionsRequest(body)
	fmt.Printf("Parsed request: ClientID=%s, Topics=%v\n", header.ClientID.content, request.TopicNames)

	return BuildDescribeTopicPartitionsResponse(request)
}

func HandleFetch(header RequestHeader, body []byte) []byte {
	fmt.Printf("Received Fetch request (version=%d, correlation_id=%d)\n",
		header.ApiVersion, header.CorrelationID)

	buf := make([]byte, 0, 1024)

	// ThrottleTimeMS (INT32) = 0
	buf = append(buf, 0x00, 0x00, 0x00, 0x00)

	// ErrorCode (INT16) = 0
	buf = append(buf, 0x00, 0x00)

	// SessionID (INT32) = 0 (no session)
	buf = append(buf, 0x00, 0x00, 0x00, 0x00)
	buf = append(buf, 0x00) // TAG_BUFFER
	fmt.Println("Hena a7oooooooooooooooooooo ")
	ResponseHeader := ResponseHeader{
		ApiKey:        header.ApiKey,
		ApiVersion:    header.ApiVersion,
		CorrelationID: header.CorrelationID,
	}
	fmt.Printf("Response Bytes: % X\n", buf)

	buf = BuildFetchResponse(ResponseHeader, ParseFetchRequest(body), buf)

	fmt.Printf("Sent Fetch response (version=%d), body size=%d\n", header.ApiVersion, len(buf))
	return buf
}

func HandleProduce(header RequestHeader, body []byte) []byte {
	fmt.Printf("Received Produce request (version=%d, correlation_id=%d)\n",
		header.ApiVersion, header.CorrelationID)

	produceReq := ParseProduceRequest(body)
	fmt.Printf("Parsed ProduceRequest: %d topics\n", len(produceReq.TopicData))
	fmt.Printf("ProduceRequest: %+v\n", produceReq)

	response := BuildProduceResponse(header, produceReq)
	return response
}
