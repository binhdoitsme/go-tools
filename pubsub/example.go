package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/binhdoitsme/pubsub/client"
	"github.com/binhdoitsme/pubsub/internal/broker"
	"github.com/binhdoitsme/pubsub/proto"
	"github.com/binhdoitsme/pubsub/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startServer() error {
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		return err
	}
	b := broker.NewBroker("./data")
	s := server.NewServer(b)
	log.Println("start server")

	if err := s.Serve(listener); err != nil {
		panic("error building server: " + err.Error())
	}
	return nil
}

func main() {
	go func() {
		err := startServer()
		if err != nil {
			panic("cannot start server")
		}
	}()
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Println("Error connecting to gRPC server: ", err.Error())
	}
	defer conn.Close()

	client := client.NewClient(conn)
	ctx := context.Background()
	stream, err := client.Subscribe(ctx, &proto.SubscribeRequest{Topic: "TestTopic", PartitionId: 3, Offset: 0})
	go func() {
		for i := range 16 {
			message := proto.Message{
				TopicName: "TestTopic",
				Key:       []byte("TopicKey"),
				Value:     fmt.Appendf(nil, `{"id": %d, "name": "Hello"}`, i),
			}
			client.Publish(ctx, &proto.PublishRequest{
				Message: &message,
			})
			time.Sleep(time.Second)
		}
	}()
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Println(err)
			return
		}
		log.Println(msg)
	}
}
