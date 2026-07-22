package rows

const (
	ItemTypeActivity = iota
	ItemTypeRequestCancel
	ItemTypeSignal
	ItemTypeTimer
	ItemTypeChildExecution
	ItemTypeBufferedEvent
	ItemTypeSignalRequested
	ItemTypeChasmNodeMetadata
	ItemTypeChasmNodeData
)

func ItemTypeName(itemType int32) string {
	switch itemType {
	case ItemTypeActivity:
		return "ActivityInfo"
	case ItemTypeTimer:
		return "TimerInfo"
	case ItemTypeChildExecution:
		return "ChildExecutionInfo"
	case ItemTypeRequestCancel:
		return "RequestCancelInfo"
	case ItemTypeSignal:
		return "SignalInfo"
	case ItemTypeSignalRequested:
		return "SignalRequested"
	case ItemTypeChasmNodeMetadata:
		return "ChasmNodeMetadata"
	case ItemTypeChasmNodeData:
		return "ChasmNodeData"
	case ItemTypeBufferedEvent:
		return "BufferedEvent"
	default:
		return "StateMapItem"
	}
}
