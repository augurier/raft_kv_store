package clientPkg

import (
	"math/rand"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

type Client struct {
	// 连接的server端节点群
	PeerIds []string
	Transport nodes.Transport
}

type Status = uint8

const (
	Ok Status = iota + 1
	NotFound
	Fail
)

func getRandomAddress(peerIds []string) string {
	// 随机选一个 id
	randomKey := peerIds[rand.Intn(len(peerIds))]
	return randomKey
}

func (client *Client) FindActiveNode() nodes.ClientInterface {
	var err error
	var c nodes.ClientInterface
	for  { // 直到找到一个可连接的节点（保证至少一个节点活着）
		peerId := getRandomAddress(client.PeerIds)
		c, err = client.Transport.DialHTTPWithTimeout("tcp", peerId)
		if err != nil {
			log.Error("dialing: ", zap.Error(err))
		} else {
			log.Sugar().Infof("client发现活跃节点[%s]", peerId)
			return c
		}
	}
}

func (client *Client) CloseRpcClient(c nodes.ClientInterface) {
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
		callErr := client.Transport.CallWithTimeout(c, "Node.WriteKV", &kvCall, &reply) // RPC
		if callErr != nil { // dial和call之间可能崩溃，重新找存活节点
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			c = client.FindActiveNode()
			continue
		}

		if !reply.Isleader { // 对方不是leader，根据反馈找leader
			leaderId := reply.LeaderId
			client.CloseRpcClient(c)
			c, err = client.Transport.DialHTTPWithTimeout("tcp", leaderId)
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
	var c nodes.ClientInterface
	for {
		c = client.FindActiveNode()

		var reply nodes.ServerReply
		callErr := client.Transport.CallWithTimeout(c, "Node.ReadKey", &key, &reply) // RPC
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
		callErr := client.Transport.CallWithTimeout(c, "Node.FindLeader", &arg, &reply) // RPC
		if callErr != nil { // dial和call之间可能崩溃，重新找存活节点
			log.Error("dialing: ", zap.Error(callErr))
			client.CloseRpcClient(c)
			c = client.FindActiveNode()
			continue
		}

		if !reply.Isleader { // 对方不是leader，根据反馈找leader
			client.CloseRpcClient(c)
			c, err = client.Transport.DialHTTPWithTimeout("tcp", reply.LeaderId)
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


