package clientPkg

import (
	"math/rand"
	"net/rpc"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

type Client struct {
	// 连接的server端节点群
	Address map[string]string
}

type Status = uint8

const (
	Ok Status = iota + 1
	NotFound
	Fail
)

func getRandomAddress(addressMap map[string]string) string {
	keys := make([]string, 0, len(addressMap))

	// 获取所有 key
	for key := range addressMap {
		keys = append(keys, key)
	}

	// 随机选一个 key
	randomKey := keys[rand.Intn(len(keys))]
	return addressMap[randomKey]
}

func (client *Client) FindActiveNode() *rpc.Client {
	var err error
	var c *rpc.Client
	for  { // 直到找到一个可连接的节点（保证至少一个节点活着）
		addr := getRandomAddress(client.Address)
		c, err = nodes.DialHTTPWithTimeout("tcp", addr)
		if err != nil {
			log.Error("dialing: ", zap.Error(err))
		} else {
			log.Sugar().Info("client发现活跃节点地址[%s]", addr)
			return c
		}
	}
}

func (client *Client) CloseRpcClient(c *rpc.Client) {
	err := c.Close()
	if err != nil {
		log.Error("client close err: ", zap.Error(err))
	}
}

func (client *Client) Write(kvCall nodes.LogEntryCall) Status {
	log.Info("client write request key :" + kvCall.LogE.Key)

	var reply nodes.ServerReply
	reply.Isleader = false
	c := client.FindActiveNode()
	var err error

	for !reply.Isleader { // 根据存活节点的反馈，直到找到leader
		callErr := nodes.CallWithTimeout(c, "Node.WriteKV", &kvCall, &reply) // RPC
		if callErr != nil { // dial和call之间可能崩溃，重新找存活节点
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			c = client.FindActiveNode()
			continue
		}

		if !reply.Isleader { // 对方不是leader，根据反馈找leader
			addr := reply.LeaderAddress
			client.CloseRpcClient(c)
			c, err = nodes.DialHTTPWithTimeout("tcp", addr)
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
		callErr := nodes.CallWithTimeout(c, "Node.ReadKey", &key, &reply) // RPC
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

func (client *Client) FindLeader() string {
	var arg struct{}
	var reply nodes.FindLeaderReply
	reply.Isleader = false
	c := client.FindActiveNode()
	var err error

	for !reply.Isleader { // 根据存活节点的反馈，直到找到leader
		callErr := nodes.CallWithTimeout(c, "Node.FindLeader", &arg, &reply) // RPC
		if callErr != nil { // dial和call之间可能崩溃，重新找存活节点
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			c = client.FindActiveNode()
			continue
		}

		if !reply.Isleader { // 对方不是leader，根据反馈找leader
			addr := client.Address[reply.LeaderId]
			client.CloseRpcClient(c)
			c, err = nodes.DialHTTPWithTimeout("tcp", addr)
			for err != nil { // 重新找下一个存活节点
				c = client.FindActiveNode()
			}
		} else { // 成功
			client.CloseRpcClient(c)
			return reply.LeaderId
		}		
	}
	log.Fatal("客户端会一直找存活节点，不会运行到这里")
	return "fault"
}


