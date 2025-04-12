package nodes

import (
	"fmt"
	"sync"
	"time"
)

type CallBehavior = uint8

const (
	NormalRpc CallBehavior = iota + 1
	DelayRpc
	RetryRpc
	FailRpc
)

// RPC 请求结构
type RPCRequest struct {
	ServiceMethod string
	Args          interface{}
	Reply 		  interface{}

	Done  chan error // 用于返回响应

	SourceId string
	// 模拟rpc请求状态
	Behavior CallBehavior
}

// 线程版 Transport
type ThreadTransport struct {
	mu       sync.Mutex
	nodeChans map[string]chan RPCRequest // 每个节点的消息通道
	connectivityMap map[string]map[string]bool // 模拟网络分区
	Ctx *Ctx
}

// 线程版 dial的返回clientinterface
type ThreadClient struct {
	SourceId  string
	TargetId  string
}

func (c *ThreadClient) Close() error {
	return nil
}

// 初始化线程通信系统
func NewThreadTransport(ctx *Ctx) *ThreadTransport {
	return &ThreadTransport{
		nodeChans: make(map[string]chan RPCRequest),
		connectivityMap: make(map[string]map[string]bool),
		Ctx: ctx,
	}
}

// 注册一个新节点chan
func (t *ThreadTransport) RegisterNodeChan(nodeId string, ch chan RPCRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodeChans[nodeId] = ch

	// 初始化连通性（默认所有节点互相可达）
	if _, exists := t.connectivityMap[nodeId]; !exists {
		t.connectivityMap[nodeId] = make(map[string]bool)
	}
	for peerId := range t.nodeChans {
		t.connectivityMap[nodeId][peerId] = true
		t.connectivityMap[peerId][nodeId] = true
	}
}

// 设置两个节点的连通性
func (t *ThreadTransport) SetConnectivity(from, to string, isConnected bool) {
    t.mu.Lock()
    defer t.mu.Unlock()
    if _, exists := t.connectivityMap[from]; exists {
        t.connectivityMap[from][to] = isConnected
    }
}

func (t *ThreadTransport) ResetConnectivity() {
    t.mu.Lock()
    defer t.mu.Unlock()
	for firstId:= range t.nodeChans {
		for peerId:= range t.nodeChans {
			if firstId != peerId {
				t.connectivityMap[firstId][peerId] = true
				t.connectivityMap[peerId][firstId] = true				
			}
		}		
	}
}

// 获取节点的 channel
func (t *ThreadTransport) getNodeChan(nodeId string) (chan RPCRequest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.nodeChans[nodeId]
	return ch, ok
}

// 模拟 Dial 操作
func (t *ThreadTransport) DialHTTPWithTimeout(network string, myId string, peerId string) (ClientInterface, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.nodeChans[peerId]; !exists {
		return nil, fmt.Errorf("节点 [%s] 不存在", peerId)
	}
	
	return &ThreadClient{SourceId: myId, TargetId: peerId}, nil
}

// 模拟 Call 操作
func (t *ThreadTransport) CallWithTimeout(client ClientInterface, serviceMethod string, args interface{}, reply interface{}) error {
	threadClient, ok := client.(*ThreadClient)
	if !ok {
		return fmt.Errorf("无效的caller")
	}

	var isConnected bool
	if threadClient.SourceId == "" {
		isConnected = true
	} else {
		t.mu.Lock()
		isConnected = t.connectivityMap[threadClient.SourceId][threadClient.TargetId]
		t.mu.Unlock()
	}
	if !isConnected {
		return fmt.Errorf("网络分区: %s cannot reach %s", threadClient.SourceId, threadClient.TargetId)
	}

	targetChan, exists := t.getNodeChan(threadClient.TargetId)
	if !exists {
		return fmt.Errorf("目标节点 [%s] 不存在", threadClient.TargetId)
	}

	done := make(chan error, 1)
	behavior := t.Ctx.GetBehavior(threadClient.SourceId, threadClient.TargetId)

	// 辅助函数：复制 replyCopy 到原始 reply
	copyReply := func(dst, src interface{}) {
		switch d := dst.(type) {
		case *AppendEntriesReply:
			*d = *(src.(*AppendEntriesReply))
		case *RequestVoteReply:
			*d = *(src.(*RequestVoteReply))
		}
	}

	sendRequest := func(req RPCRequest, ch chan RPCRequest) bool {
		select {
		case ch <- req:
			return true
		default:
			return false
		}
	}

	switch behavior {
	case RetryRpc:
		retryTimes, ok := t.Ctx.GetRetries(threadClient.SourceId, threadClient.TargetId)
		if !ok {
			log.Fatal("没有设置对应的retry次数")
		}

		var lastErr error
		for i := 0; i < retryTimes; i++ {
			var replyCopy interface{}
			useCopy := true

			switch r := reply.(type) {
			case *AppendEntriesReply:
				tmp := *r
				replyCopy = &tmp
			case *RequestVoteReply:
				tmp := *r
				replyCopy = &tmp
			default:
				replyCopy = reply // 其他类型不复制
				useCopy = false
			}

			request := RPCRequest{
				ServiceMethod: serviceMethod,
				Args:          args,
				Reply:         replyCopy,
				Done:          done,
				SourceId:      threadClient.SourceId,
				Behavior:      NormalRpc,
			}

			if !sendRequest(request, targetChan) {
				return fmt.Errorf("目标节点 [%s] 无法接收请求", threadClient.TargetId)
			}

			select {
			case err := <-done:
				if err == nil && useCopy {
					copyReply(reply, replyCopy)
				}
				if err == nil {
					return nil
				}
				lastErr = err
			case <-time.After(250 * time.Millisecond):
				lastErr = fmt.Errorf("RPC 调用超时: %s", serviceMethod)
			}
		}
		return lastErr

	default:
		request := RPCRequest{
			ServiceMethod: serviceMethod,
			Args:          args,
			Reply:         reply,
			Done:          done,
			SourceId:      threadClient.SourceId,
			Behavior:      behavior,
		}

		if !sendRequest(request, targetChan) {
			return fmt.Errorf("目标节点 [%s] 无法接收请求", threadClient.TargetId)
		}

		select {
		case err := <-done:
			return err
		case <-time.After(250 * time.Millisecond):
			return fmt.Errorf("RPC 调用超时: %s", serviceMethod)
		}
	}
}