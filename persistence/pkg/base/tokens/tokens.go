package tokens

import (
	"encoding/json"
	"time"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

type HistoryNodePageToken struct {
	LastNodeID int64
	LastTxnID  int64
}

func (pt *HistoryNodePageToken) Serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *HistoryNodePageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type HistoryTreeBranchPageToken struct {
	ShardID  int32
	TreeID   string
	BranchID string
}

func (pt *HistoryTreeBranchPageToken) Serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *HistoryTreeBranchPageToken) Deserialize(payload []byte) error {
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

type ClusterMetadataPageToken struct {
	ClusterName string
}

func (pt *ClusterMetadataPageToken) Serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *ClusterMetadataPageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type TaskPageToken struct {
	TaskID int64
}

func (t *TaskPageToken) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TaskPageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, t)
	} else {
		return nil
	}
}

type ScheduledTaskPageToken struct {
	TaskID    int64
	Timestamp time.Time
}

func (t *ScheduledTaskPageToken) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *ScheduledTaskPageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, t)
	} else {
		return nil
	}
}

func GetImmediateTaskNextPageToken(lastTaskID int64, exclusiveMaxTaskID int64) ([]byte, error) {
	nextTaskID := lastTaskID + 1
	if nextTaskID < exclusiveMaxTaskID {
		token := TaskPageToken{TaskID: nextTaskID}
		return token.Serialize()
	}
	return nil, nil
}

func GetImmediateTaskReadRange(request *p.GetHistoryTasksRequest) (inclusiveMinTaskID int64, exclusiveMaxTaskID int64, err error) {
	inclusiveMinTaskID = request.InclusiveMinTaskKey.TaskID
	if len(request.NextPageToken) > 0 {
		var token TaskPageToken
		if err = token.Deserialize(request.NextPageToken); err != nil {
			return 0, 0, err
		}
		inclusiveMinTaskID = token.TaskID
	}
	return inclusiveMinTaskID, request.ExclusiveMaxTaskKey.TaskID, nil
}

type TaskQueueUserDataPageToken struct {
	LastTaskQueueName string
}

func (pt *TaskQueueUserDataPageToken) Serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *TaskQueueUserDataPageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type MatchingTaskPageToken struct {
	TaskID int64
}

func (pt *MatchingTaskPageToken) Serialize() ([]byte, error) {
	return json.Marshal(pt)
}

func (pt *MatchingTaskPageToken) Deserialize(payload []byte) error {
	if len(payload) > 0 {
		return json.Unmarshal(payload, pt)
	} else {
		return nil
	}
}

type ClusterMembersPageToken struct {
	LastSeenHostID primitives.UUID
}

func (pt *ClusterMembersPageToken) Serialize() ([]byte, error) {
	return []byte(pt.LastSeenHostID.String()), nil
}

func (pt *ClusterMembersPageToken) Deserialize(payload []byte) error {
	if lastSeenHostID, err := primitives.ParseUUID(string(payload)); err != nil {
		return err
	} else {
		pt.LastSeenHostID = lastSeenHostID
		return nil
	}
}

type QueueV2PageToken struct {
	LastReadQueueName string
}

func (qt *QueueV2PageToken) Serialize() ([]byte, error) {
	return []byte(qt.LastReadQueueName), nil
}

func (qt *QueueV2PageToken) Deserialize(payload []byte) error {
	qt.LastReadQueueName = string(payload)
	return nil
}

type NexusEndpointsPageToken struct {
	LastSeenEndpointID string
}

func (pt *NexusEndpointsPageToken) Serialize() []byte {
	return []byte(pt.LastSeenEndpointID)
}

func (pt *NexusEndpointsPageToken) Deserialize(payload []byte) {
	pt.LastSeenEndpointID = string(payload)
}
