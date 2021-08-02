package esqd

// 这是一个小根堆的实现
type inFlightPqueue []*Message

func newInFlightPqueue(cap int) inFlightPqueue {
	return make(inFlightPqueue, 0, cap)
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
