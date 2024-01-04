package raft

import (
	"github.com/better-concurrent/guc"
	"github.com/exerosis/RabiaGo/rabia"
	"math"
	"sync"
	"sync/atomic"
)

type test struct {
	log *rabia.Log

	queue       *guc.PriorityBlockingQueue
	messages    map[uint64][]byte
	proposeLock sync.RWMutex

	committed  uint64
	highest    int64
	commitLock sync.Mutex
}

func (node *test) Size() uint32 {
	return node.log.Size
}

func (node *test) Propose(id uint64, data []byte) error {
	go func() {
		node.proposeLock.Lock()
		node.messages[id] = data
		node.proposeLock.Unlock()
		node.queue.Offer(rabia.Identifier{Value: id})
	}()
	return nil
}

func (node *test) Run() error {
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		var current = uint64(0)
		for {
			var message = node.queue.Take().(rabia.Identifier).Value
			node.log.Logs[current%uint64(node.log.Size)] = message
			var value = atomic.LoadInt64(&node.highest)
			for value < int64(current) && !atomic.CompareAndSwapInt64(&node.highest, value, int64(current)) {
				value = atomic.LoadInt64(&node.highest)
			}
			current++
			var committed = atomic.LoadUint64(&node.committed)
			if current-committed >= uint64(node.log.Size) {
				panic("WRAPPED TOO HARD!")
			}
			node.log.Logs[current%uint64(node.log.Size)] = 0
		}
	}()
	group.Wait()
	return nil
}

func (node *test) Consume(block func(uint64, uint64, []byte) error) error {
	node.commitLock.Lock()
	defer node.commitLock.Unlock()
	var highest = atomic.LoadInt64(&node.highest)
	for i := atomic.LoadUint64(&node.committed); int64(i) <= highest; i++ {
		var slot = i % uint64(len(node.log.Logs))
		var proposal = node.log.Logs[slot]
		if proposal == 0 {
			highest = int64(i)
			//if we hit the first unfilled slot stop
			break
		}
		if proposal != math.MaxUint64 {
			node.proposeLock.Lock()
			data, present := node.messages[proposal]
			if present {
				delete(node.messages, proposal)
			}
			node.proposeLock.Unlock()
			if present {
				reason := block(i, proposal, data)
				if reason != nil {
					return reason
				}
			}
		}
	}
	atomic.StoreUint64(&node.committed, uint64(highest+1))
	return nil
}

func (node *test) Repair(index uint64) (uint64, []byte, error) {
	//TODO implement me
	panic("implement me")
}
