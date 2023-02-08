package rabia

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/better-concurrent/guc"
	"github.com/exerosis/RabiaGo/rabia"
	"go.uber.org/multierr"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Message struct {
	Data    []byte
	Context context.Context
}

type RabiaNode struct {
	Log          *rabia.Log
	Queues       []*guc.PriorityBlockingQueue
	Messages     map[uint64]Message
	ProposeMutex sync.RWMutex
	Pipes        []uint16
	Addresses    []string
	Committed    uint64
	Highest      int64
	spreader     *rabia.TcpMulticaster
}

const INFO = false

func MakeRabiaNode(addresses []string, pipes ...uint16) *RabiaNode {
	var compare = &comparator{comparingProposals}
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]*guc.PriorityBlockingQueue, len(pipes))
	for i := range queues {
		queues[i] = guc.NewPriorityBlockingQueueWithComparator(compare)
	}
	return &RabiaNode{
		rabia.MakeLog(uint16(len(addresses)), size), queues,
		make(map[uint64]Message), sync.RWMutex{},
		pipes, addresses, uint64(0), int64(-1), nil,
	}
}

func (node *RabiaNode) Propose(
	context context.Context, id uint64, data []byte,
) error {
	context.Done()
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header[0:], id)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data)))
	node.ProposeMutex.Lock()
	var send = append(header, data...)
	for node.spreader == nil {
		time.Sleep(time.Millisecond)
	}
	reason := node.spreader.Send(send)
	if reason != nil {
		return reason
	}
	node.Messages[id] = Message{Data: data, Context: context}
	node.ProposeMutex.Unlock()
	node.Queues[id>>32%uint64(len(node.Queues))].Offer(id)
	return nil
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

	var others []string
	for _, other := range node.Addresses {
		if other != address {
			others = append(others, other)
		}
	}
	spreader, reason := rabia.TCP(address, 2000, others...)
	if reason != nil {
		return reason
	}
	node.spreader = spreader
	for _, inbound := range spreader.Inbound {
		go func(inbound net.Conn) {
			for {
				var fill = func(buffer []byte) {
					for i := 0; i < len(buffer); {
						amount, reason := inbound.Read(buffer)
						if reason != nil {
							panic(reason)
						}
						i += amount
					}
				}
				var header = make([]byte, 12)
				fill(header)
				var id = binary.LittleEndian.Uint64(header[0:])
				var data = make([]byte, binary.LittleEndian.Uint32(header[8:]))
				fill(data)
				node.ProposeMutex.Lock()
				node.Messages[id] = Message{Data: data, Context: context.Background()}
				node.ProposeMutex.Unlock()
				node.Queues[id>>32%uint64(len(node.Queues))].Offer(id)
			}
		}(inbound)
	}

	//var mark = time.Now().UnixNano()
	for index, pipe := range node.Pipes {
		go func(index int, pipe uint16, queue *guc.PriorityBlockingQueue) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint64(index)
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
				var next = queue.Poll()
				if next == nil {
					println("considering noop")
					time.Sleep(50 * time.Nanosecond)
					next = queue.Poll()
					if next == nil {
						next = uint64(math.MaxUint64)
					} else {
						println("Second time avoided noop")
					}
				}
				return uint16(current % uint64(log.Size)), next.(uint64), nil
			}, func(slot uint16, message uint64) error {
				//if message != math.MaxUint64 {
				//	fmt.Printf("[Pipe-%d] %d\n", index, message)
				//}
				log.Logs[current%uint64(log.Size)] = message
				var value = atomic.LoadInt64(&node.Highest)
				for value < int64(current) && !atomic.CompareAndSwapInt64(&node.Highest, value, int64(current)) {
					value = atomic.LoadInt64(&node.Highest)
				}
				current += uint64(len(node.Pipes))
				log.Logs[current%uint64(log.Size)] = 0
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
