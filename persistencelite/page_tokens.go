package persistencelite

import (
	"encoding/json"
	"time"

	p "go.temporal.io/server/common/persistence"
)

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
	ShardID  int32
	TreeID   string
	BranchID string
}

func (pt *historyTreeBranchPageToken) serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *historyTreeBranchPageToken) deserialize(payload []byte) error {
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, pt); err != nil {
			return err
		}
	}
	if pt.ShardID == 0 {
		pt.ShardID = 1
	}
	return nil
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

func getImmediateTaskNextPageToken(
	lastTaskID int64,
	exclusiveMaxTaskID int64,
) ([]byte, error) {
	nextTaskID := lastTaskID + 1
	if nextTaskID < exclusiveMaxTaskID {
		token := taskPageToken{TaskID: nextTaskID}
		return token.serialize()
	}
	return nil, nil
}

func getImmediateTaskReadRange(
	request *p.GetHistoryTasksRequest,
) (inclusiveMinTaskID int64, exclusiveMaxTaskID int64, err error) {
	inclusiveMinTaskID = request.InclusiveMinTaskKey.TaskID
	if len(request.NextPageToken) > 0 {
		var token taskPageToken
		if err = token.deserialize(request.NextPageToken); err != nil {
			return 0, 0, err
		}
		inclusiveMinTaskID = token.TaskID
	}
	return inclusiveMinTaskID, request.ExclusiveMaxTaskKey.TaskID, nil
}
