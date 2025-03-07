package clientPkg

import (
	"net/rpc"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

type Client struct {
	// 连接的server端节点（node1）
	ServerId string
	Address string
}

type Status = uint8

const (
	Ok Status = iota + 1
	NotFound
	Fail
)

func (client *Client) Write(kvCall nodes.LogEntryCall) Status {
	log.Info("client write request key :" + kvCall.LogE.Key)
	c, err := rpc.DialHTTP("tcp", client.Address)
	if err != nil {
		log.Error("dialing: ", zap.Error(err))
		return Fail
	}

	defer func(server *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(c)

	var reply nodes.ServerReply
	callErr := c.Call("Node.WriteKV", kvCall, &reply) // RPC
	if callErr != nil {
		log.Error("dialing: ", zap.Error(callErr))
		return Fail
	}

	if reply.Isconnect { // 发送成功
		return Ok
	} else { // 失败
		return Fail
	}
}

func (client *Client) Read(key string, value *string) Status { // 查不到value为空
	log.Info("client read request key :" + key)
	if value == nil {
        return Fail
    }

	c, err := rpc.DialHTTP("tcp", client.Address)
	if err != nil {
		log.Error("dialing: ", zap.Error(err))
		return Fail
	}

	defer func(server *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(c)

	var reply nodes.ServerReply
	callErr := c.Call("Node.ReadKey", key, &reply) // RPC
	if callErr != nil {
		log.Error("dialing: ", zap.Error(callErr))
		return Fail
	}

	if reply.Isconnect { // 发送成功
		if reply.HaveValue {
			*value = reply.Value
			return Ok
		} else {
			return NotFound
		}
	} else { // 失败
		return Fail
	}
}


