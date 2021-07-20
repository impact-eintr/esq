package esqd

type dummyBackendQueue struct {
	readCh chan []byte
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readCh: make(chan []byte)}
}

func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *dummyBackendQueue) ReadChan() chan []byte {
	return d.readCh
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}
