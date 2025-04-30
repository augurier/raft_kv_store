package nodes

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
)

// 真实rpc通讯的transport类型实现
type HTTPTransport struct{
	NodeMap map[string]string // id到addr的映射
}

// 封装有超时的dial
func (t *HTTPTransport) DialHTTPWithTimeout(network string, myId string, peerId string) (ClientInterface, error) {
	done := make(chan struct{})
	var client *rpc.Client
	var err error

	go func() {
		client, err = rpc.DialHTTP(network, t.NodeMap[peerId])
		close(done)
	}()

	select {
	case <-done:
		return &HTTPClient{rpcClient: client}, err
	case <-time.After(50 * time.Millisecond):
		return nil, fmt.Errorf("dial timeout: %s", t.NodeMap[peerId])
	}
}

func (t *HTTPTransport) CallWithTimeout(clientInterface ClientInterface, serviceMethod string, args interface{}, reply interface{}) error {
    c, ok := clientInterface.(*HTTPClient)
	client := c.rpcClient
    if !ok {
        return fmt.Errorf("invalid client type")
    }

	done := make(chan error, 1)

	go func() {
		switch serviceMethod {
		case "Node.AppendEntries":
			arg, ok := args.(*AppendEntriesArg)
			resp, ok2 := reply.(*AppendEntriesReply)
			if !ok || !ok2 {
				done <- errors.New("type assertion failed for AppendEntries")
				return
			}
			done <- client.Call(serviceMethod, arg, resp)

		case "Node.RequestVote":
			arg, ok := args.(*RequestVoteArgs)
			resp, ok2 := reply.(*RequestVoteReply)
			if !ok || !ok2 {
				done <- errors.New("type assertion failed for RequestVote")
				return
			}
			done <- client.Call(serviceMethod, arg, resp)

		case "Node.WriteKV":
			arg, ok := args.(*LogEntryCall)
			resp, ok2 := reply.(*ServerReply)
			if !ok || !ok2 {
				done <- errors.New("type assertion failed for WriteKV")
				return
			}
			done <- client.Call(serviceMethod, arg, resp)

		case "Node.ReadKey":
			arg, ok := args.(*string)
			resp, ok2 := reply.(*ServerReply)
			if !ok || !ok2 {
				done <- errors.New("type assertion failed for ReadKey")
				return
			}
			done <- client.Call(serviceMethod, arg, resp)

		case "Node.FindLeader":
			arg, ok := args.(struct{})
			resp, ok2 := reply.(*FindLeaderReply)
			if !ok || !ok2 {
				done <- errors.New("type assertion failed for FindLeader")
				return
			}
			done <- client.Call(serviceMethod, arg, resp)

		default:
			done <- fmt.Errorf("unknown service method: %s", serviceMethod)
		}
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(50 * time.Millisecond): // 设置超时时间
		return fmt.Errorf("call timeout: %s", serviceMethod)
	}
}

type HTTPClient struct {
    rpcClient *rpc.Client
}

func (h *HTTPClient) Close() error {
    return h.rpcClient.Close()
}


