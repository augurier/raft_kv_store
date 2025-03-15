package nodes

import "strconv"

type CallMode = uint8
const (
	Normal CallMode = iota + 1
	Delay
	Fail
)

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
	LogE LogEntry
	CallState CallMode
}

type KVReply struct {
	Reply bool
}

type LogIdAndEntry struct {
	LogId int
	Entry LogEntry
}