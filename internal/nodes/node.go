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
	mu    sync.Mutex
	// 当前节点id
	selfId string
	// 记录的leader(不能用votedfor：投票的leader可能没有收到多数票)
	leaderId string

	// 除当前节点外其他节点id
	nodes []string

	// 当前节点状态
	state State

	// 任期
	currTerm int

	// 简单的kv存储
	log []RaftLogEntry

	// leader用来标记新log, = log.len
	maxLogId int

	// 已提交的index
	commitIndex int

	// 最后应用（写到db）的index
	lastApplied int	

	// 需要发送给每个节点的下一个索引
	nextIndex map[string]int

	// 已经发送给每个节点的最大索引
	matchIndex map[string]int

	// 存kv（模拟状态机）
	db *leveldb.DB
	// 持久化节点数据（currterm votedfor log）
	storage *RaftStorage

	votedFor string
	electionTimer *time.Timer

	// 通信方式
	transport Transport
}

