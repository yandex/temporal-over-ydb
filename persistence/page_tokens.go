package persistence

import (
	"encoding/json"
	"time"
)

type taskQueueUserDataPageToken struct {
	LastTaskQueueName string
}

func (pt *taskQueueUserDataPageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *taskQueueUserDataPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type historyNodePageToken struct {
	LastNodeID int64
	LastTxnID  int64
}

func (pt *historyNodePageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *historyNodePageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type historyTreeBranchPageToken struct {
	TreeID   string
	BranchID string
}

func (pt *historyTreeBranchPageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *historyTreeBranchPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type clusterMetadataPageToken struct {
	ClusterName string
}

func (pt *clusterMetadataPageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *clusterMetadataPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type matchingTaskPageToken struct {
	TaskID int64
}

func (pt *matchingTaskPageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *matchingTaskPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type taskPageToken struct {
	TaskID int64
}

func (t *taskPageToken) serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *taskPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, t)
	} else {
		return nil
	}
}

type scheduledTaskPageToken struct {
	TaskID    int64
	Timestamp time.Time
}

func (t *scheduledTaskPageToken) serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *scheduledTaskPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, t)
	} else {
		return nil
	}
}
