package rabia

import (
	"context"
	"fmt"
	"github.com/better-concurrent/guc"
	"github.com/cornelk/hashmap"
	"github.com/exerosis/RabiaGo/rabia"
	"go.uber.org/multierr"
	"sync"
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
	Queues    []*guc.PriorityBlockingQueue
	Messages  *hashmap.Map[uint64, Message]
	Pipes     []uint16
	Addresses []string
	Committed uint64
	Highest   uint64
}

const INFO = true

func MakeRabiaNode(addresses []string, pipes ...uint16) *RabiaNode {
	var compare = &comparator{comparingProposals}
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]*guc.PriorityBlockingQueue, len(pipes))
	for i := range queues {
		queues[i] = guc.NewPriorityBlockingQueueWithComparator(compare)
	}
	return &RabiaNode{
		rabia.MakeLog(uint16(len(addresses)), size), queues,
		hashmap.New[uint64, Message](),
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
	//messages map ig?

	//var mark = time.Now().UnixNano()
	for index, pipe := range node.Pipes {
		go func(index int, pipe uint16, queue *guc.PriorityBlockingQueue) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint32(index)
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
				var next = queue.Take().(uint64)
				return uint16(current % log.Size), next, nil
			}, func(slot uint16, message uint64) error {
				fmt.Println("Got:")
				element, exists := node.Messages.Get(message)
				if exists {
					println(string(element.Data))
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
		}(index, pipe, node.Queues[index])
	}
	group.Wait()
	fmt.Println("Exiting finally!")
	return reasons
}
