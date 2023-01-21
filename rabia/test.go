package rabia

import (
	"context"
	"fmt"
	"github.com/better-concurrent/guc"
	"github.com/exerosis/RabiaGo/rabia"
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
)

func MakeMessage(data []byte, context context.Context) Message {
	return Message{data, context}
}

type Message struct {
	Data    []byte
	Context context.Context
}

type RabiaNode struct {
	Log       *rabia.Log
	Queue     *guc.PriorityBlockingQueue
	Messages  *guc.ConcurrentHashMap
	Pipes     []uint16
	Addresses []string
	Committed uint64
	Highest   uint64
}

const INFO = true

func MakeRabiaNode(addresses []string, pipes ...uint16) *RabiaNode {
	var compare = &comparator{comparingProposals}
	var size = uint32((65536 / len(pipes)) * len(pipes))
	return &RabiaNode{
		rabia.MakeLog(uint16(len(addresses)), size),
		guc.NewPriorityBlockingQueueWithComparator(compare),
		guc.NewConcurrentHashMap(100_000, 10_000),
		pipes, addresses, uint64(0), uint64(0),
	}
}

func (node *RabiaNode) Run(
	address string,
) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(node.Pipes))
	var log = node.Log
	var instances = make([][]uint64, len(node.Pipes))
	//messages map ig?

	//var mark = time.Now().UnixNano()
	var count = uint32(0)
	for index, pipe := range node.Pipes {
		go func(index int, pipe uint16, instance []uint64) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint32(index)
			var i = 0
			proposals, reason := rabia.TCP(address, pipe+1, node.Addresses...)
			states, reason := rabia.TCP(address, pipe+2, node.Addresses...)
			votes, reason := rabia.TCP(address, pipe+3, node.Addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
			info("Connected!\n")
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64, error) {
				return uint16(current % log.Size), instance[i], nil
			}, func(slot uint16, message uint64) error {
				fmt.Println("Working?")
				var amount = atomic.AddUint32(&count, 1)
				//for amount >= AVERAGE && !atomic.CompareAndSwapUint32(&count, amount, 0) {
				//	amount = atomic.LoadUint32(&count)
				//}
				//if amount >= AVERAGE {
				//	percent, reason := cpu.Percent(0, false)
				//	if reason != nil {
				//		return reason
				//	}
				//	var duration = time.Since(time.Unix(0, atomic.LoadInt64(&mark)))
				//	atomic.StoreInt64(&mark, time.Now().UnixNano())
				//	var throughput = float64(amount) / duration.Seconds()
				//	fmt.Printf("%d - %.2f\n", uint32(throughput), percent[0])
				//}
				i++
				if i == len(instance)-1 {
					fmt.Println("Done! ", index)
					return fmt.Errorf("done: %d", amount)
				}
				current += uint32(len(node.Pipes))
				return nil
			}, info)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("running smr pipe %d: %s", index, reason)
				reasons = result
			}
			return
		}(index, pipe, instances[index])
	}
	group.Wait()
	fmt.Println("Exiting finally!")
	return reasons
}
