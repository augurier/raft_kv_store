package fuzz

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"simple-kv-store/threadTest"
	"strconv"
)

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
			peerIds = append(peerIds, strconv.Itoa(int(seed)) + "." + strconv.Itoa(i+1))
		}

		ctx := nodes.NewCtx()
		threadTransport := nodes.NewThreadTransport(ctx)
		var quitCollections []chan struct{}
		var nodeCollections []*nodes.Node

		for i := 0; i < n; i++ {
			node, quitChan := threadTest.ExecuteNodeI(strconv.Itoa(int(seed)) + "." + strconv.Itoa(i+1), false, peerIds, threadTransport)
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
		threadTest.CheckZeroOrOneLeader(t, nodeCollections)

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
func injectRandomBehavior(ctx *nodes.Ctx, r *rand.Rand, peers []string) (map[string]bool) {
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
