package pqueue

import "container/heap"

//  priority queue
type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

type PriorityQueue []*Item

func New(cap int) PriorityQueue {
	return make(PriorityQueue, 0, cap)

}

func (pq PriorityQueue) Len() int {
	return len(pq)

}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority

}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j

}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(PriorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[:n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item

}

func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(PriorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[:n-1]
	return item

}

func (pq *PriorityQueue) PeekAndShift(max int64) (*Item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}
	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0) // pq实现了 Push Pop Less Swap Len 所以是一个 heap.Interface{}
	return item, 0

}
