package nodes

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
)

// RaftStorage 结构，持久化 currentTerm、votedFor 和 logEntries
type RaftStorage struct {
	mu          sync.Mutex
	filePath    string
	CurrentTerm int         `json:"current_term"`
	VotedFor    string      `json:"voted_for"`
	LogEntries  []RaftLogEntry  `json:"log_entries"`
}

// NewRaftStorage 创建 Raft 存储
func NewRaftStorage(filePath string) *RaftStorage {
	storage := &RaftStorage{
		filePath: filePath,
	}
	storage.loadData() // 载入已有数据
	return storage
}

// loadData 读取 JSON 文件数据
func (rs *RaftStorage) loadData() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	file, err := os.Open(rs.filePath)
	if err != nil {
		log.Info("文件未创建：" + rs.filePath)
		rs.saveData() // 文件不存在时创建默认数据
		return
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(rs)
	if err != nil {
		log.Error("读取文件失败：" + rs.filePath, zap.Error(err))
	}
}

// 持久化数据到 JSON(必须持有锁，不能直接外部调用)
func (rs *RaftStorage) saveData() {
	// 获取文件所在的目录
	dir := filepath.Dir(rs.filePath)

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Error("创建存储目录失败", zap.Error(err))
		return
	}

	file, err := os.Create(rs.filePath)
	if err != nil {
		log.Error("持久化节点出错", zap.Error(err))
		return
	}
	defer file.Close()

	err = json.NewEncoder(file).Encode(rs)
	if err != nil {
		log.Error("持久化写入失败")
	}
}

// SetCurrentTerm 设置当前 term，并清空 votedFor（符合 Raft 规范）
func (rs *RaftStorage) SetCurrentTerm(term int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if term > rs.CurrentTerm {
		rs.CurrentTerm = term
		rs.VotedFor = "" // 新任期清空投票
		rs.saveData()
	}
}

// GetCurrentTerm 获取当前 term
func (rs *RaftStorage) GetCurrentTerm() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.CurrentTerm
}

// SetVotedFor 记录投票给谁
func (rs *RaftStorage) SetVotedFor(candidate string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.VotedFor = candidate
	rs.saveData()
}

// GetVotedFor 获取投票对象
func (rs *RaftStorage) GetVotedFor() string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.VotedFor
}

// 同时设置
func (rs *RaftStorage) SetTermAndVote(term int, candidate string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.VotedFor = candidate
	rs.CurrentTerm = term
	rs.saveData()
}

// append日志
func (rs *RaftStorage) AppendLog(rlogE RaftLogEntry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.LogEntries = append(rs.LogEntries, rlogE)
	rs.saveData()
}

// 更改日志
func (rs *RaftStorage) WriteLog(rlogEs []RaftLogEntry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.LogEntries = rlogEs
	rs.saveData()
}

// 获取所有日志
func (rs *RaftStorage) GetLogEntries() []RaftLogEntry {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.LogEntries
}

// GetLastLogIndex 获取最新日志的 index
func (rs *RaftStorage) GetLastLogIndex() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.LogEntries) == 0 {
		return 0
	}
	return len(rs.LogEntries)-1
}
