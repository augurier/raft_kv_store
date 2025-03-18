package clientPkg

import (
	"net/rpc"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

type Client struct {
	// 连接的server端节点群
	Address [] string
}

type Status = uint8

const (
	Ok Status = iota + 1
	NotFound
	Fail
)

func (client *Client) FindActiveNode() *rpc.Client {
	var err error
	var c *rpc.Client
	i := 0
	for { // 直到找到一个可连接的节点（保证至少一个节点活着）
		c, err = rpc.DialHTTP("tcp", client.Address[i])
		if err != nil {
			log.Error("dialing: ", zap.Error(err))
			i++
		} else {
			break
		}
		if i == len(client.Address) {
			log.Fatal("没找到存活节点")
		}
	}
	return c
}

func (client *Client) CloseRpcClient(c *rpc.Client) {
	err := c.Close()
	if err != nil {
		log.Error("client close err: ", zap.Error(err))
	}
}
// 
func (client *Client) Write(kvCall nodes.LogEntryCall) Status {
	log.Info("client write request key :" + kvCall.LogE.Key)

	var reply nodes.ServerReply
	reply.Isleader = false
	c := client.FindActiveNode()
	var err error

	for !reply.Isleader { // 根据存活节点的反馈，直到找到leader
		callErr := c.Call("Node.WriteKV", kvCall, &reply) // RPC
		if callErr != nil { // dial和call之间可能崩溃，重新找存活节点
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			c = client.FindActiveNode()
			continue
		}

		if !reply.Isleader { // 对方不是leader，根据反馈找leader
			addr := reply.LeaderAddress
			client.CloseRpcClient(c)
			c, err = rpc.DialHTTP("tcp", addr)
			for err != nil { // 重新找下一个存活节点
				c = client.FindActiveNode()
			}
		} else { // 成功
			client.CloseRpcClient(c)
			return Ok
		}		
	}
	log.Fatal("客户端会一直找存活节点，不会运行到这里")
	return Fail
}

func (client *Client) Read(key string, value *string) Status { // 查不到value为空
	log.Info("client read request key :" + key)
	if value == nil {
        return Fail
    }
	var c *rpc.Client
	for {
		c = client.FindActiveNode()

		var reply nodes.ServerReply
		callErr := c.Call("Node.ReadKey", key, &reply) // RPC
		if callErr != nil {
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			continue
		}

		// 目前一定发送成功
		if reply.HaveValue {
			*value = reply.Value
			client.CloseRpcClient(c)
			return Ok
		} else {
			client.CloseRpcClient(c)
			return NotFound
		}		
	}
	
}


