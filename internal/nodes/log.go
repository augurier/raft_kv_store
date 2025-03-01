package nodes

type LogEntry struct {
	Key string
	Value string
}

type KVReply struct {
	Reply bool
}

type LogIdAndEntry struct {
	LogId int
	Entry LogEntry
}