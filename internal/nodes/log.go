package nodes

import "strconv"

type LogEntry struct {
	Key string
	Value string
}
func (LogE *LogEntry) print() string {
	return "key: " + LogE.Key + ", value: " + LogE.Value
}

type RaftLogEntry struct {
	LogE LogEntry
	LogId int
	Term int
}
func (RLogE *RaftLogEntry) print() string {
	return "logid: " + strconv.Itoa(RLogE.LogId) + ", term: " + strconv.Itoa(RLogE.Term) + ", " + RLogE.LogE.print()
}

type LogEntryCall struct {
	Id LogEntryCallId
	LogE LogEntry
}

type LogEntryCallId struct {
	ClientId string
	LogId int
}

type KVReply struct {
	Reply bool
}

type LogIdAndEntry struct {
	LogId int
	Entry LogEntry
}