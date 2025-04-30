package nodes

import (
	"fmt"
	"sync"
	"time"
)

// Ctx 结构体：管理不同节点之间的通信行为
type Ctx struct {
	mu        sync.Mutex
	Behavior  map[string]CallBehavior       // (src,target) -> CallBehavior
	Delay     map[string]time.Duration      // (src,target) -> 延迟时间
	Retries   map[string]int				// 记录 (src,target) 的重发调用次数
}

// NewCtx 创建上下文
func NewCtx() *Ctx {
	return &Ctx{
		Behavior:  make(map[string]CallBehavior),
		Delay:     make(map[string]time.Duration),
		Retries:   make(map[string]int),
	}
}

// SetBehavior 设置 A->B 的 RPC 行为
func (c *Ctx) SetBehavior(src, dst string, behavior CallBehavior, delay time.Duration, retries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%s->%s", src, dst)
	c.Behavior[key] = behavior
	c.Delay[key] = delay
	c.Retries[key] = retries
}

// GetBehavior 获取 A->B 的行为
func (c *Ctx) GetBehavior(src, dst string) (CallBehavior) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%s->%s", src, dst)
	if state, exists := c.Behavior[key]; exists {
		return state
	}
	return NormalRpc
}

func (c *Ctx) GetDelay(src, dst string) (t time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%s->%s", src, dst)
	if t, ok = c.Delay[key]; ok {
		return t, ok
	}
	return 0, ok
}

func (c *Ctx) GetRetries(src, dst string) (times int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%s->%s", src, dst)
	if times, ok = c.Retries[key]; ok {
		return times, ok
	}
	return 0, ok
}