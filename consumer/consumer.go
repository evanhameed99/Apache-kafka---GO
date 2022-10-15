package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "test_topic", 0)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("connected to test_topic")
		conn.SetReadDeadline(time.Now().Add(time.Second * 10))

		// Below will read first message from the stream
		message, _ := conn.ReadMessage(1e6)
		fmt.Println(string(message.Value))

		// Reading all messages from stream

		batch := conn.ReadBatch(1e3, 1e9)
		bytes := make([]byte, 1e3)
		for {
			_, err := batch.Read(bytes)
			if err != nil {
				break
			}
			fmt.Println(string(bytes))
		}
	}

}
