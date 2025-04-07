package nodes

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

// RaftStorage 结构，持久化 currentTerm、votedFor 和 logEntries
type RaftStorage struct {
	mu       sync.Mutex
	db       *leveldb.DB
	filePath string
	isfinish bool
}

// NewRaftStorage 创建 Raft 存储
func NewRaftStorage(filePath string) *RaftStorage {
	db, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		log.Fatal("无法打开 LevelDB:", zap.Error(err))
	}

	return &RaftStorage{
		db:       db,
		filePath: filePath,
		isfinish: false,
	}
}

// SetCurrentTerm 设置当前 term
func (rs *RaftStorage) SetCurrentTerm(term int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.isfinish {
		return
	}
	err := rs.db.Put([]byte("current_term"), []byte(strconv.Itoa(term)), nil)
	if err != nil {
		log.Error("SetCurrentTerm 持久化失败:", zap.Error(err))
	}
}

// GetCurrentTerm 获取当前 term
func (rs *RaftStorage) GetCurrentTerm() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	data, err := rs.db.Get([]byte("current_term"), nil)
	if err != nil {
		return 0 // 默认 term = 0
	}
	term, _ := strconv.Atoi(string(data))
	return term
}

// SetVotedFor 记录投票给谁
func (rs *RaftStorage) SetVotedFor(candidate string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.isfinish {
		return
	}
	err := rs.db.Put([]byte("voted_for"), []byte(candidate), nil)
	if err != nil {
		log.Error("SetVotedFor 持久化失败:", zap.Error(err))
	}
}

// GetVotedFor 获取投票对象
func (rs *RaftStorage) GetVotedFor() string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	data, err := rs.db.Get([]byte("voted_for"), nil)
	if err != nil {
		return ""
	}
	return string(data)
}

// SetTermAndVote 原子更新 term 和 vote
func (rs *RaftStorage) SetTermAndVote(term int, candidate string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.isfinish {
		return
	}

	batch := new(leveldb.Batch)
	batch.Put([]byte("current_term"), []byte(strconv.Itoa(term)))
	batch.Put([]byte("voted_for"), []byte(candidate))

	err := rs.db.Write(batch, nil) // 原子提交
	if err != nil {
		log.Error("SetTermAndVote 持久化失败:", zap.Error(err))
	}
}

// AppendLog 追加日志
func (rs *RaftStorage) AppendLog(entry RaftLogEntry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.db == nil {
		return
	}

	// 序列化日志
	batch := new(leveldb.Batch)
	data, _ := json.Marshal(entry)
	key := "log_" + strconv.Itoa(entry.LogId)
	batch.Put([]byte(key), data)

	lastIndex := strconv.Itoa(entry.LogId)
	batch.Put([]byte("last_log_index"), []byte(lastIndex))
	err := rs.db.Write(batch, nil)
	if err != nil {
		log.Error("AppendLog 持久化失败:", zap.Error(err))
	}
}

// GetLastLogIndex 获取最新日志的 index
func (rs *RaftStorage) GetLastLogIndex() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	data, err := rs.db.Get([]byte("last_log_index"), nil)
	if err != nil {
		return -1
	}
	index, _ := strconv.Atoi(string(data))
	return index
}

// WriteLog 批量写入日志（保证原子性）
func (rs *RaftStorage) WriteLog(entries []RaftLogEntry) {
	if len(entries) == 0 {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.isfinish {
		return
	}

	batch := new(leveldb.Batch)
	for _, entry := range entries {
		data, _ := json.Marshal(entry)
		key := "log_" + strconv.Itoa(entry.LogId)
		batch.Put([]byte(key), data)
	}

	// 更新最新日志索引
	lastIndex := strconv.Itoa(entries[len(entries)-1].LogId)
	batch.Put([]byte("last_log_index"), []byte(lastIndex))

	err := rs.db.Write(batch, nil)
	if err != nil {
		log.Error("WriteLog 持久化失败:", zap.Error(err))
	}
}

// GetLogEntries 获取所有日志
func (rs *RaftStorage) GetLogEntries() []RaftLogEntry {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var logs []RaftLogEntry
	iter := rs.db.NewIterator(nil, nil) // 遍历所有键值
	defer iter.Release()

	for iter.Next() {
		key := string(iter.Key())
		if strings.HasPrefix(key, "log_") { // 过滤日志 key
			var entry RaftLogEntry
			if err := json.Unmarshal(iter.Value(), &entry); err == nil {
				logs = append(logs, entry)
			} else {
				log.Error("解析日志失败:", zap.Error(err))
			}
		}
	}

	return logs
}

// Close 关闭数据库
func (rs *RaftStorage) Close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.db.Close()
	rs.isfinish = true
}
