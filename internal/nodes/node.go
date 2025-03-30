package nodes

import (
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type State = uint8

const (
	Follower State = iota + 1
	Candidate
	Leader
)

type Node struct {
	Mu    sync.Mutex
	// 当前节点id
	SelfId string
	// 记录的leader(不能用votedfor：投票的leader可能没有收到多数票)
	LeaderId string

	// 除当前节点外其他节点id
	Nodes []string

	// 当前节点状态
	State State

	// 任期
	CurrTerm int

	// 简单的kv存储
	Log []RaftLogEntry

	// leader用来标记新log, = log.len
	MaxLogId int

	// 已提交的index
	CommitIndex int

	// 最后应用（写到db）的index
	LastApplied int	

	// 需要发送给每个节点的下一个索引
	NextIndex map[string]int

	// 已经发送给每个节点的最大索引
	MatchIndex map[string]int

	// 存kv（模拟状态机）
	Db *leveldb.DB
	// 持久化节点数据（currterm votedfor log）
	Storage *RaftStorage

	VotedFor string
	ElectionTimer *time.Timer

	// 通信方式
	Transport Transport
}

