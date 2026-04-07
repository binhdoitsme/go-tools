package client

import (
	"github.com/binhdoitsme/pubsub/proto"
	"google.golang.org/grpc"
)

type Client struct {
	proto.PubSubClient
}

func NewClient(cc grpc.ClientConnInterface) *Client {
	return &Client{proto.NewPubSubClient(cc)}
}
