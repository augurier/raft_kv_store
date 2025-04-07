package nodes

import (
	"math/rand"
	"sync"
	"time"
)

type RandomTimeTable struct {
	Mu    sync.Mutex
	electionTimeOut time.Duration
	israndom bool
	// heartbeat 50ms
	// rpcTimeout 50ms
	// follower变candidate 500ms
	// 等待选举成功时间 300ms
}

func NewRTTable() *RandomTimeTable {
	return &RandomTimeTable{
		israndom: true,
	}
}

func (rttable *RandomTimeTable) GetElectionTimeout() time.Duration {
	rttable.Mu.Lock()
	defer rttable.Mu.Unlock()
	if rttable.israndom {
		return time.Duration(500+rand.Intn(500)) * time.Millisecond
	} else {
		return rttable.electionTimeOut
	}
}

func (rttable *RandomTimeTable) SetElectionTimeout(t time.Duration) {
	rttable.Mu.Lock()
	defer rttable.Mu.Unlock()
	rttable.israndom = false
	rttable.electionTimeOut = t
}

func (rttable *RandomTimeTable) ResetElectionTimeout() {
	rttable.Mu.Lock()
	defer rttable.Mu.Unlock()
	rttable.israndom = true
}