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

func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	// 如果没有空间了 先扩容
	if n+1 > c {
		npq := make(inFlightPqueue, n, c*2)
		// 复制
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	x.index = n
	(*pq)[n] = x // push到数组尾部
	pq.up(n)
}

func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	pq.Swap(0, n-1) // 交换堆顶元素与堆底元素
	pq.down(0, n-1) // 向下调整
	if n < (c/2) && c > 25 {
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	//删除堆底元素
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[:n-1]
	return x
}

// 向上调整指定节点
func (pq *inFlightPqueue) up(j int) {
	for {
		i := (j - 1) / 2                            // 父节点
		if i == j || (*pq)[j].pri >= (*pq)[i].pri { // 单节点 || 子节点 >= 父节点
			break
		}
		pq.Swap(i, j)
		j = i
	}
}

// 向下调整指定节点
func (pq *inFlightPqueue) down(i, n int) {
	for {
		j1 := 2*i + 1          // 左孩子
		if j1 >= n || j1 < 0 { // j1已经是叶子节点了
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // 右孩子
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break // 如果父节点比孩子节点小 则不交换
		}
		pq.Swap(i, j)
		i = j
	}
}
