package nodes

import (
	"math/rand"
	"time"
)

type RandomTimeTable struct {
	electionTimeOut time.Duration
	israndom bool
	// heartbeat 50ms
	// rpcTimeout 100ms
	// follower变candidate 500ms
	// 等待选举成功时间 300ms
}

func NewRTTable() *RandomTimeTable {
	return &RandomTimeTable{
		israndom: true,
	}
}

func (rttable *RandomTimeTable) GetElectionTimeout() time.Duration {
	if rttable.israndom {
		return time.Duration(500+rand.Intn(500)) * time.Millisecond
	} else {
		return time.Duration(rttable.electionTimeOut)
	}
}

func (rttable *RandomTimeTable) SetElectionTimeout(t time.Duration) {
	rttable.israndom = false
	rttable.electionTimeOut = t
}

func (rttable *RandomTimeTable) ResetElectionTimeout() {
	rttable.israndom = true
}