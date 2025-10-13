package main

import (
	"fmt"
	"net"
	"os"

	"kafgo/app/metadata"
	"kafgo/app/server"
)

func main() {
	fmt.Println("Kafka-like broker started on :9092")

	// Load metadata at startup
	metadata.LoadClusterMetadata()

	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go server.HandleConnection(conn)
	}
}
