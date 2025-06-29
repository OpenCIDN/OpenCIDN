package model

import (
	"database/sql/driver"
	"time"
)

type MessageStatus uint64

const (
	StatusPending    MessageStatus = 0
	StatusProcessing MessageStatus = 10
	StatusCompleted  MessageStatus = 20
	StatusFailed     MessageStatus = 30
	StatusCleanup    MessageStatus = 90
)

type Message struct {
	MessageID     int64
	Content       string
	Lease         string
	Priority      int
	Status        MessageStatus
	Data          MessageAttr
	LastHeartbeat time.Time
}

const (
	KindBlob     = "blob"
	KindManifest = "manifest"
)

type MessageAttr struct {
	Kind string `json:"kind"`

	Error string `json:"error,omitempty"`

	Host     string `json:"host,omitempty"`
	Image    string `json:"image,omitempty"`
	Progress int64  `json:"progress,omitempty"`
	Size     int64  `json:"size,omitempty"`

	Deep bool `json:"deep,omitempty"`
}

func (n *MessageAttr) Scan(value any) error {
	if value == nil {
		return nil
	}
	*n = unmarshal[MessageAttr](asString(value))
	return nil
}

func (n MessageAttr) Value() (driver.Value, error) {
	return marshal(n), nil
}
