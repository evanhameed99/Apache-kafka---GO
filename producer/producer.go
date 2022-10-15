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
	}

	fmt.Println("Connected to topic")
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	conn.WriteMessages(kafka.Message{Value: []byte("Hello dear kafka topic, I am here!")})
}
