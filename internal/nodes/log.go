package nodes

const (
	Normal State = iota + 1
	Delay
	Fail
)

type LogEntry struct {
	Key string
	Value string
}

type LogEntryCall struct {
	LogE LogEntry
	CallState State
}

type KVReply struct {
	Reply bool
}

type LogIdAndEntry struct {
	LogId int
	Entry LogEntry
}