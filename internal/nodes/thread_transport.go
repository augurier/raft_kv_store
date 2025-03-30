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
	connectivityMap map[string]map[string]bool // 模拟网络分区
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
func NewThreadTransport() *ThreadTransport {
	return &ThreadTransport{
		nodeChans: make(map[string]chan RPCRequest),
		connectivityMap: make(map[string]map[string]bool),
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
	if threadClient.SourceId == "" { // 来自客户端的连接
		isConnected = true
	} else {
		t.mu.Lock()
		isConnected = t.connectivityMap[threadClient.SourceId][threadClient.TargetId] // 检查连通性
		t.mu.Unlock()		
	}


    if !isConnected {
        return fmt.Errorf("network partition: %s cannot reach %s", threadClient.SourceId, threadClient.TargetId)
    }

	// 获取目标节点的 channel
	targetChan, exists := t.getNodeChan(threadClient.TargetId)
	if !exists {
		return fmt.Errorf("目标节点 [%s] 不存在", threadClient.TargetId)
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
			if threadClient.SourceId == "" { // 来自客户端的连接
				isConnected = true
			} else {
				t.mu.Lock()
				isConnected = t.connectivityMap[threadClient.TargetId][threadClient.SourceId] // 检查连通性
				t.mu.Unlock()		
			}

			if !isConnected {
				return fmt.Errorf("network partition: %s cannot reach %s", threadClient.TargetId, threadClient.SourceId)
			}
			return err
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("RPC 调用超时: %s", serviceMethod)
		}
	default:
		return fmt.Errorf("目标节点 [%s] 无法接收请求", threadClient.TargetId)
	}
}
