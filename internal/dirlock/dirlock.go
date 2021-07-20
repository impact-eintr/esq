package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

type DirLock struct {
	dir string
	f   *os.File
}

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

func (dl *DirLock) Lock() error {
	f, err := os.Open(dl.dir)
	if err != nil {
		return err
	}

	dl.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", dl.dir, err)
	}
	return nil

}

func (dl *DirLock) Unlock() error {
	defer dl.f.Close()
	return syscall.Flock(int(dl.f.Fd()), syscall.LOCK_UN)
}
