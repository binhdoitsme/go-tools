package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/binhdoitsme/pubsub/core"
	"github.com/binhdoitsme/pubsub/internal/broker"
	"github.com/binhdoitsme/pubsub/proto"
	"google.golang.org/grpc"
)

const (
	flushInterval = 5_000 // fixed interval flushing for demo
)

type Server struct {
	proto.PubSubServer
	grpcServer *grpc.Server
	broker     *broker.Broker
}

func NewServer(broker *broker.Broker) *Server {
	gs := grpc.NewServer()
	server := &Server{grpcServer: gs, broker: broker}
	proto.RegisterPubSubServer(gs, server)
	return server
}

func (s *Server) Publish(ctx context.Context, request *proto.PublishRequest) (resp *proto.PublishResponse, err error) {
	now := time.Now().UTC().UnixNano()
	reqMessage := request.GetMessage()
	msg := core.Message{
		TopicName:   reqMessage.GetTopicName(),
		PartitionID: int(reqMessage.GetPartitionId()),
		Offset:      reqMessage.GetOffset(),
		Key:         reqMessage.GetKey(),
		Value:       reqMessage.GetValue(),
		Timestamp:   now,
		Headers:     reqMessage.GetHeaders(),
	}
	msgf, err := s.broker.Commit(&msg)
	if err != nil {
		return resp, err
	}
	resp = &proto.PublishResponse{}
	resp.Success = true
	resp.Offset = msgf.Offset
	resp.PartitionId = int32(msg.PartitionID)
	return resp, nil
}

const refreshIntervalSec = 1

func (s *Server) Subscribe(request *proto.SubscribeRequest, stream grpc.ServerStreamingServer[proto.SubscribeResponse]) error {
	msgChan := make(chan core.Message)
	done := make(chan struct{})
	ticker := time.NewTicker(refreshIntervalSec * time.Second)
	// refresh coroutine
	var lock sync.RWMutex
	currentOffset := request.GetOffset()

	go func() {
		for {
			select {
			case <-done:
				close(msgChan)
				return
			case <-ticker.C:
				lock.RLock()
				msgs := s.broker.Fetch(request.GetTopic(), int(request.GetPartitionId()), currentOffset)
				for _, msg := range msgs {
					msgChan <- msg
				}
				currentOffset += uint64(len(msgs))
				lock.RUnlock()
			}
		}
	}()

	for msg := range msgChan {
		toSend := &proto.Message{
			TopicName:   msg.TopicName,
			PartitionId: int32(msg.PartitionID),
			Offset:      msg.Offset,
			Key:         msg.Key,
			Value:       msg.Value,
			Timestamp:   msg.Timestamp,
			Headers:     msg.Headers,
		}
		resp := &proto.SubscribeResponse{Message: toSend}
		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Serve(lis net.Listener) error {
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		s.broker.Flush()
		os.Exit(0)
	}()
	return s.grpcServer.Serve(lis)
}
