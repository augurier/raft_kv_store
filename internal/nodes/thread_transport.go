package nodes

import (
	"fmt"
	"sync"
	"time"
)

// RPC 请求结构
type RPCRequest struct {
	ServiceMethod string
	Args          interface{}
	Reply 		  interface{}
	Done  chan error // 用于返回响应
}

// 线程版 Transport
type ThreadTransport struct {
	mu       sync.Mutex
	nodeChans map[string]chan RPCRequest // 每个节点的消息通道
}

// 线程版 dial的返回clientinterface
type ThreadClient struct {
	targetId  string
}

func (c *ThreadClient) Close() error {
	return nil
}

// 初始化线程通信系统
func NewThreadTransport() *ThreadTransport {
	return &ThreadTransport{
		nodeChans: make(map[string]chan RPCRequest),
	}
}

// 注册一个新节点chan
func (t *ThreadTransport) RegisterNodeChan(nodeId string, ch chan RPCRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodeChans[nodeId] = ch
}

// 获取节点的 channel
func (t *ThreadTransport) getNodeChan(nodeId string) (chan RPCRequest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, ok := t.nodeChans[nodeId]
	return ch, ok
}

// 模拟 Dial 操作
func (t *ThreadTransport) DialHTTPWithTimeout(network string, peerId string) (ClientInterface, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.nodeChans[peerId]; !exists {
		return nil, fmt.Errorf("节点 [%s] 不存在", peerId)
	}
	
	return &ThreadClient{targetId: peerId}, nil
}

// 模拟 Call 操作
func (t *ThreadTransport) CallWithTimeout(client ClientInterface, serviceMethod string, args interface{}, reply interface{}) error {
	threadClient, ok := client.(*ThreadClient)
	if !ok {
		return fmt.Errorf("无效的客户端")
	}

	// 获取目标节点的 channel
	targetChan, exists := t.getNodeChan(threadClient.targetId)
	if !exists {
		return fmt.Errorf("目标节点 [%s] 不存在", threadClient.targetId)
	}

	// 创建响应通道（用于返回 RPC 结果）
	done := make(chan error, 1)

	// 发送请求
	request := RPCRequest{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:  done,
	}

	select {
	case targetChan <- request:
		// 等待响应或超时
		select {
		case err := <-done:
			return err
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("RPC 调用超时: %s", serviceMethod)
		}
	default:
		return fmt.Errorf("目标节点 [%s] 无法接收请求", threadClient.targetId)
	}
}
