# pubsub

A Pub/Sub message broker service with inspiration from Kafka. Built in Go and support data exchange via gRPC using Protobuf.

## Usage

Here are quick examples showing how to (1) start a pubsub server and (2) consume from a running server using the provided client.

1. Start a server

    Create and start a server programmatically (or see `example.go`):

    ```go
    package main

    import (
        "log"
        "net"

        "github.com/binhdoitsme/pubsub/internal/broker"
        "github.com/binhdoitsme/pubsub/server"
    )

    func main() {
        listener, err := net.Listen("tcp", "localhost:8080")
        if err != nil {
            log.Fatal(err)
        }

        b := broker.NewBroker("./data")
        s := server.NewServer(b)
        log.Println("starting pubsub server on :8080")
        if err := s.Serve(listener); err != nil {
            log.Fatal(err)
        }
    }
    ```

    Run it:

    ```bash
    go run example.go
    # or go run ./cmd/myserver (if you place server code elsewhere)
    ```

2. Consume an existing server using the provisioned client

    Use the generated gRPC client to publish and subscribe. Example usage:

    ```go
    package main

    import (
        "context"
        "log"
        "time"

        "github.com/binhdoitsme/pubsub/client"
        "github.com/binhdoitsme/pubsub/proto"
        "google.golang.org/grpc"
        "google.golang.org/grpc/credentials/insecure"
    )

    func main() {
        conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Fatal(err)
        }
        defer conn.Close()

        c := client.NewClient(conn)
        ctx := context.Background()

        // Publish a message
        _, err = c.Publish(ctx, &proto.PublishRequest{Message: &proto.Message{
            TopicName: "TestTopic",
            Key:       []byte("k"),
            Value:     []byte("hello world"),
            Timestamp: time.Now().UnixMilli(),
        }})
        if err != nil {
            log.Fatal(err)
        }

        // Subscribe (streaming)
        stream, err := c.Subscribe(ctx, &proto.SubscribeRequest{Topic: "TestTopic", PartitionId: 0, Offset: 0})
        if err != nil {
            log.Fatal(err)
        }
        for {
            resp, err := stream.Recv()
            if err != nil {
                log.Println("stream error:", err)
                break
            }
            log.Printf("received message: %+v\n", resp.GetMessage())
        }
    }
    ```

    Run the consumer program similarly with `go run`.

    Notes:

    - The repository includes an `example.go` demonstrating both server and client flows.

    - Regenerate Go code from `pb/pubsub.proto` if you change the proto definitions: `protoc --go_out=. --go-grpc_out=. pb/pubsub.proto`.
