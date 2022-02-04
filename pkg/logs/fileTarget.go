package logs

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type fileTarget struct {
	sync.RWMutex
	Filename string `json:"filename"`
	fd       *os.File
	curSize  int64
	Level    int
	MaxSize  int64 `json:"max_size"`
	Rotate   bool  `json:"rotate"`
	MaxFiles int   `json:"max_files"`
	openTime time.Time
}

func init() {
	RegisterTareget(TARGET_FILE, &fileTarget{})
}

func (f *fileTarget) WriteMsg(data logData) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	f.Lock()
	defer f.Unlock()

	// 旋转日志
	if f.Rotate && f.curSize > 0 && f.curSize >= f.MaxSize {
		f.rotateFile()
	}

	prefix := logPrefix(data.level)
	now := utils.CurDatetime()
	s := fmt.Sprintf("[%s] [%s] [%s] %s \n", prefix, now, data.category, data.msg)
	nbyte, err := f.fd.WriteString(s)
	if err != nil {
		panic(err)
	}
	if nbyte != len(s) {
		panic(fmt.Sprintf("Unable to export whole log through file! Wrote %v out of %v bytes.", nbyte, len(s)))
	}
	fileInfo, err := f.fd.Stat()
	if err != nil {
		panic(err)
	}
	f.curSize = fileInfo.Size()

}

// 打开文件 初始化 fileTarget 的各项属性
func (f *fileTarget) Init(config string) error {
	if err := json.Unmarshal([]byte(config), &f); err != nil {
		return err
	}
	if f.Filename == "" {
		return fmt.Errorf("File target init failed; error:%v", "Filename is Empty")
	}

	fd, err := openFile(f.Filename)
	if err != nil {
		return err
	}

	f.fd = fd
	fileInfo, err := f.fd.Stat()
	if err != nil {
		return err
	}
	f.curSize = fileInfo.Size()
	return nil

}

func (f *fileTarget) rotateFile() {
	defer func() {
		// 确保文件描述符是打开的
		if _, err := os.Lstat(f.Filename); err != nil {
			f.fd, _ = openFile(f.Filename)
		}
	}()

	// 默认最大文件数 5
	if f.MaxFiles <= 1 {
		f.MaxFiles = 5
	}

	// 先关闭文件描述符
	f.fd.Close()
	for i := f.MaxFiles; i >= 0; i-- {
		var fName string
		if i == 0 {
			fName = f.Filename
		} else {
			fName = f.Filename + "." + strconv.Itoa(i)
		}

		if _, err := os.Lstat(fName); err == nil {
			if i == f.MaxFiles {
				// 删除最后一个日志文件
				os.Remove(fName)
				continue
			}

			// 日志文件向后移动(重命名),例如3个日志文件,2->3,1->2,0->1
			newFName := f.Filename + "." + strconv.Itoa(i+1)
			if err := os.Rename(fName, newFName); err != nil {
				panic(err)
			}

			// 重新创建第一个文件
			if i == 0 {
				f.fd, _ = openFile(fName)
			}
		}
	}
}

// 读写 追加 生成
func openFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.ModePerm)
}
