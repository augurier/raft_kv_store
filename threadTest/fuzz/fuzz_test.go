package fuzz

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	clientPkg "simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"simple-kv-store/threadTest"
	"strconv"
)

// 1.针对随机配置随机消息状态
func FuzzRaftBasic(f *testing.F) {
	var seenSeeds sync.Map
	// 添加初始种子
	f.Add(int64(1))
	fmt.Println("Running")

	f.Fuzz(func(t *testing.T, seed int64) {
		if _, loaded := seenSeeds.LoadOrStore(seed, true); loaded {
			t.Skipf("Seed %d already tested, skipping...", seed)
			return
		}
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("goroutine panic: %v\n%s", r, debug.Stack())
				f, _ := os.Create("panic_goroutine.log")
				fmt.Fprint(f, msg)
				f.Close()
			}
		}()

		r := rand.New(rand.NewSource(seed)) // 使用局部 rand

		n := 3 + 2*(r.Intn(4))
		fmt.Printf("随机了%d个节点\n", n)
		logs := (r.Intn(10))
		fmt.Printf("随机了%d份日志\n", logs)
		var peerIds []string
		for i := 0; i < n; i++ {
			peerIds = append(peerIds, strconv.Itoa(int(seed))+"."+strconv.Itoa(i+1))
		}

		ctx := nodes.NewCtx()
		threadTransport := nodes.NewThreadTransport(ctx)
		var quitCollections []chan struct{}
		var nodeCollections []*nodes.Node

		for i := 0; i < n; i++ {
			node, quitChan := threadTest.ExecuteNodeI(strconv.Itoa(int(seed))+"."+strconv.Itoa(i+1), false, peerIds, threadTransport)
			nodeCollections = append(nodeCollections, node)
			node.RTTable.SetElectionTimeout(750 * time.Millisecond)
			quitCollections = append(quitCollections, quitChan)
		}

		// 模拟 a-b 通讯行为
		faultyNodes := injectRandomBehavior(ctx, r, peerIds)

		time.Sleep(time.Second)

		clientObj := clientPkg.NewClient("0", peerIds, threadTransport)

		for i := 0; i < logs; i++ {
			key := fmt.Sprintf("k%d", i)
			log := nodes.LogEntry{Key: key, Value: "v"}
			clientObj.Write(log)
		}

		time.Sleep(time.Second)

		var rightNodeCollections []*nodes.Node
		for _, node := range nodeCollections {
			if !faultyNodes[node.SelfId] {
				rightNodeCollections = append(rightNodeCollections, node)
			}
		}
		threadTest.CheckSameLog(t, rightNodeCollections)
		threadTest.CheckLeaderInvariant(t, nodeCollections)

		for _, quitChan := range quitCollections {
			close(quitChan)
		}
		time.Sleep(time.Second)
		for i := 0; i < n; i++ {
			// 确保完成退出
			nodeCollections[i].Mu.Lock()
			if !nodeCollections[i].IsFinish {
				nodeCollections[i].IsFinish = true
			}
			nodeCollections[i].Mu.Unlock()

			os.RemoveAll("leveldb/simple-kv-store" + strconv.Itoa(int(seed)) + "." + strconv.Itoa(i+1))
			os.RemoveAll("storage/node" + strconv.Itoa(int(seed)) + "." + strconv.Itoa(i+1))
		}
	})
}

// 注入节点间行为
func injectRandomBehavior(ctx *nodes.Ctx, r *rand.Rand, peers []string) map[string]bool /*id:Isfault*/ {
	behaviors := []nodes.CallBehavior{
		nodes.FailRpc,
		nodes.DelayRpc,
		nodes.RetryRpc,
	}
	n := len(peers)
	maxFaulty := r.Intn(n/2 + 1) // 随机选择 0 ~ n/2 个出问题的节点

	// 随机选择出问题的节点
	shuffled := append([]string(nil), peers...)
	r.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	faultyNodes := make(map[string]bool)
	for i := 0; i < maxFaulty; i++ {
		faultyNodes[shuffled[i]] = true
	}

	for _, one := range peers {
		if faultyNodes[one] {
			b := behaviors[r.Intn(len(behaviors))]
			delay := time.Duration(r.Intn(100)) * time.Millisecond

			switch b {
			case nodes.FailRpc:
				fmt.Printf("[%s]的异常行为是fail\n", one)
			case nodes.DelayRpc:
				fmt.Printf("[%s]的异常行为是delay\n", one)
			case nodes.RetryRpc:
				fmt.Printf("[%s]的异常行为是retry\n", one)
			}

			for _, two := range peers {
				if one == two {
					continue
				}

				if faultyNodes[one] && faultyNodes[two] {
					ctx.SetBehavior(one, two, nodes.FailRpc, 0, 0)
					ctx.SetBehavior(one, two, nodes.FailRpc, 0, 0)
				} else {
					ctx.SetBehavior(one, two, b, delay, 2)
					ctx.SetBehavior(two, one, b, delay, 2)
				}
			}
		}

	}
	return faultyNodes
}

// 2.对一个长时间运行的系统，注入随机行为
func FuzzRaftRobust(f *testing.F) {
	var seenSeeds sync.Map
	var fuzzMu sync.Mutex
	// 添加初始种子
	f.Add(int64(0))
	fmt.Println("Running")
	n := 5

	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i+1))
	}

	ctx := nodes.NewCtx()
	threadTransport := nodes.NewThreadTransport(ctx)
	quitCollections := make(map[string]chan struct{})
	nodeCollections := make(map[string]*nodes.Node)

	for i := 0; i < n; i++ {
		id := strconv.Itoa(i+1)
		node, quitChan := threadTest.ExecuteNodeI(id, false, peerIds, threadTransport)
		nodeCollections[id] = node
		quitCollections[id] = quitChan
	}

	f.Fuzz(func(t *testing.T, seed int64) {
		fuzzMu.Lock()
		defer fuzzMu.Unlock()
		if _, loaded := seenSeeds.LoadOrStore(seed, true); loaded {
			t.Skipf("Seed %d already tested, skipping...", seed)
			return
		}
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("goroutine panic: %v\n%s", r, debug.Stack())
				f, _ := os.Create("panic_goroutine.log")
				fmt.Fprint(f, msg)
				f.Close()
			}
		}()

		r := rand.New(rand.NewSource(seed)) // 使用局部 rand

		clientObj := clientPkg.NewClient("0", peerIds, threadTransport)

		faultyNodes := injectRandomBehavior2(ctx, r, peerIds, threadTransport, quitCollections)

		key := fmt.Sprintf("k%d", seed % 10)
		log := nodes.LogEntry{Key: key, Value: "v"}
		clientObj.Write(log)

		time.Sleep(time.Second)

		var rightNodeCollections []*nodes.Node
		for _, node := range nodeCollections {
			_, exist := faultyNodes[node.SelfId]
			if !exist {
				rightNodeCollections = append(rightNodeCollections, node)
			}
		}
		threadTest.CheckLogInvariant(t, rightNodeCollections)
		threadTest.CheckLeaderInvariant(t, rightNodeCollections)

		// ResetFaultyNodes
		threadTransport.ResetConnectivity()
		for id, isrestart := range faultyNodes {
			if !isrestart {
				for _, peerIds := range peerIds {
					if id == peerIds {
						continue
					}
					ctx.SetBehavior(id, peerIds, nodes.NormalRpc, 0, 0)
					ctx.SetBehavior(peerIds, id, nodes.NormalRpc, 0, 0)
				}
			} else {
				newNode, quitChan := threadTest.ExecuteNodeI(id, true, peerIds, threadTransport)
				quitCollections[id] = quitChan
				nodeCollections[id] = newNode
			}
			fmt.Printf("[%s]恢复异常\n", id)
		}
	})
	for _, quitChan := range quitCollections {
		close(quitChan)
	}
	time.Sleep(time.Second)
	for id, node := range nodeCollections {
		// 确保完成退出
		node.Mu.Lock()
		if !node.IsFinish {
			node.IsFinish = true
		}
		node.Mu.Unlock()

		os.RemoveAll("leveldb/simple-kv-store" + id)
		os.RemoveAll("storage/node" + id)
	}
}

// 3.综合
func FuzzRaftPlus(f *testing.F) {
	var seenSeeds sync.Map
	// 添加初始种子
	f.Add(int64(0))
	fmt.Println("Running")

	f.Fuzz(func(t *testing.T, seed int64) {
		if _, loaded := seenSeeds.LoadOrStore(seed, true); loaded {
			t.Skipf("Seed %d already tested, skipping...", seed)
			return
		}
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("goroutine panic: %v\n%s", r, debug.Stack())
				f, _ := os.Create("panic_goroutine.log")
				fmt.Fprint(f, msg)
				f.Close()
			}
		}()

		r := rand.New(rand.NewSource(seed)) // 使用局部 rand

		n := 3 + 2*(r.Intn(4))
		fmt.Printf("随机了%d个节点\n", n)
		ElectionTimeOut := 500 + r.Intn(500)
		fmt.Printf("随机的投票超时时间：%d\n", ElectionTimeOut)

		var peerIds []string
		for i := 0; i < n; i++ {
			peerIds = append(peerIds, strconv.Itoa(int(seed))+"."+strconv.Itoa(i+1))
		}

		ctx := nodes.NewCtx()
		threadTransport := nodes.NewThreadTransport(ctx)
		quitCollections := make(map[string]chan struct{})
		nodeCollections := make(map[string]*nodes.Node)

		for i := 0; i < n; i++ {
			id := strconv.Itoa(int(seed))+"."+strconv.Itoa(i+1)
			node, quitChan := threadTest.ExecuteNodeI(id, false, peerIds, threadTransport)
			nodeCollections[id] = node
			node.RTTable.SetElectionTimeout(time.Duration(ElectionTimeOut) * time.Millisecond)
			quitCollections[id] = quitChan
		}

		clientObj := clientPkg.NewClient("0", peerIds, threadTransport)

		for i := 0; i < 5; i++ { // 模拟10次异常
			fmt.Printf("第%d轮异常注入开始\n", i + 1)
			faultyNodes := injectRandomBehavior2(ctx, r, peerIds, threadTransport, quitCollections)

			key := fmt.Sprintf("k%d", i)
			log := nodes.LogEntry{Key: key, Value: "v"}
			clientObj.Write(log)

			time.Sleep(time.Second)
			
			var rightNodeCollections []*nodes.Node
			for _, node := range nodeCollections {
				_, exist := faultyNodes[node.SelfId]
				if !exist {
					rightNodeCollections = append(rightNodeCollections, node)
				}
			}
			threadTest.CheckLogInvariant(t, rightNodeCollections)
			threadTest.CheckLeaderInvariant(t, rightNodeCollections)

			// ResetFaultyNodes
			threadTransport.ResetConnectivity()
			for id, isrestart := range faultyNodes {
				if !isrestart {
					for _, peerId := range peerIds {
						if id == peerId {
							continue
						}
						ctx.SetBehavior(id, peerId, nodes.NormalRpc, 0, 0)
						ctx.SetBehavior(peerId, id, nodes.NormalRpc, 0, 0)
					} 
				} else {
					newNode, quitChan := threadTest.ExecuteNodeI(id, true, peerIds, threadTransport)
					quitCollections[id] = quitChan
					nodeCollections[id] = newNode
				}
				fmt.Printf("[%s]恢复异常\n", id)
			}
			
		}

		for _, quitChan := range quitCollections {
			close(quitChan)
		}
		time.Sleep(time.Second)
		for id, node := range nodeCollections {
			// 确保完成退出
			node.Mu.Lock()
			if !node.IsFinish {
				node.IsFinish = true
			}
			node.Mu.Unlock()

			os.RemoveAll("leveldb/simple-kv-store" + id)
			os.RemoveAll("storage/node" + id)
		}
	})
}

func injectRandomBehavior2(ctx *nodes.Ctx, r *rand.Rand, peers []string, tran *nodes.ThreadTransport, quitCollections map[string]chan struct{}) map[string]bool /*id:needRestart*/ {

	n := len(peers)
	maxFaulty := r.Intn(n/2 + 1) // 随机选择 0 ~ n/2 个出问题的节点

	// 随机选择出问题的节点
	shuffled := append([]string(nil), peers...)
	r.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	faultyNodes := make(map[string]bool)
	for i := 0; i < maxFaulty; i++ {
		faultyNodes[shuffled[i]] = false
	}
	PartitionNodes := make(map[string]bool)

	for _, one := range peers {
		_, exist := faultyNodes[one]
		if exist {
			b := r.Intn(5)

			switch b {
			case 0:
				fmt.Printf("[%s]的异常行为是fail\n", one)
				for _, two := range peers {
					if one == two {
						continue
					}
					ctx.SetBehavior(one, two, nodes.FailRpc, 0, 0)
					ctx.SetBehavior(two, one, nodes.FailRpc, 0, 0)
				}
			case 1:
				fmt.Printf("[%s]的异常行为是delay\n", one)
				t := r.Intn(100)
				fmt.Printf("[%s]的delay time = %d\n", one, t)
				delay := time.Duration(t) * time.Millisecond
				for _, two := range peers {
					if one == two {
						continue
					}
					_, exist2 := faultyNodes[two]
					if exist2 {
						ctx.SetBehavior(one, two, nodes.FailRpc, 0, 0)
						ctx.SetBehavior(two, one, nodes.FailRpc, 0, 0)
					} else {
						ctx.SetBehavior(one, two, nodes.DelayRpc, delay, 0)
						ctx.SetBehavior(two, one, nodes.DelayRpc, delay, 0)
					}
				}
			case 2:
				fmt.Printf("[%s]的异常行为是retry\n", one)
				for _, two := range peers {
					if one == two {
						continue
					}
					_, exist2 := faultyNodes[two]
					if exist2 {
						ctx.SetBehavior(one, two, nodes.FailRpc, 0, 0)
						ctx.SetBehavior(two, one, nodes.FailRpc, 0, 0)
					} else {
						ctx.SetBehavior(one, two, nodes.RetryRpc, 0, 2)
						ctx.SetBehavior(two, one, nodes.RetryRpc, 0, 2)
					}
				}
			case 3:
				fmt.Printf("[%s]的异常行为是stop\n", one)
				faultyNodes[one] = true
				close(quitCollections[one])

			case 4:
				fmt.Printf("[%s]的异常行为是partition\n", one)
				PartitionNodes[one] = true
			}
		}
	}
	for id, _ := range PartitionNodes {
		for _, two := range peers {
			if !PartitionNodes[two] {
				tran.SetConnectivity(id, two, false)
				tran.SetConnectivity(two, id, false)
			}
		}
	}

	return faultyNodes
}
