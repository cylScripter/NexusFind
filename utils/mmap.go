package utils

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

const AppendData int64 = 1024 * 1024
const (
	ModeAppend = iota //文件追加
	ModeCreate        // 打开文件时清空文件
)

type Mmap struct {
	MmapBytes   []byte
	FileName    string
	FileLen     int64
	FilePointer int64
	MapType     int64
	FileFd      *os.File
}

func NewMmap(fileName string, mode int) (*Mmap, error) {
	this := &Mmap{MmapBytes: make([]byte, 0), FileName: fileName, FileLen: 0, MapType: 0, FileFd: nil}
	fileMode := os.O_RDWR
	fileCreateMode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	if mode == ModeCreate {
		fileMode = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(fileName, fileMode, 0664)
	if err != nil {
		f, err = os.OpenFile(fileName, fileCreateMode, 0664)
		if err != nil {
			return nil, err
		}
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()
	this.FileLen = size
	if mode == ModeCreate || fi.Size() == 0 {
		if err := syscall.Ftruncate(int(f.Fd()), 0+AppendData); err != nil {
			f.Close()
			return nil, err
		}
		this.FileLen = AppendData
	}
	this.MmapBytes, err = syscall.Mmap(int(f.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	if size == 0 {
		this.WriteInt64(0, 8)
	}
	this.FilePointer = this.ReadInt64(0)
	this.FileFd = f
	return this, nil
}

func (m *Mmap) SetFilePointer(fileLen int64) {
	m.FilePointer = fileLen
}

func (m *Mmap) checkFilePointer(checkValue int64) error {
	if m.FilePointer+checkValue >= m.FileLen {
		err := syscall.Ftruncate(int(m.FileFd.Fd()), m.FileLen+AppendData)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		m.FileLen += AppendData
		err = syscall.Munmap(m.MmapBytes)
		if err != nil {
			return err
		}
		m.MmapBytes, err = syscall.Mmap(int(m.FileFd.Fd()), 0, int(m.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}
	}
	return nil
}

func (m *Mmap) checkFileCap(start, lens int64) error {
	if start+lens >= m.FileLen {
		err := syscall.Ftruncate(int(m.FileFd.Fd()), m.FileLen+AppendData)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		m.FileLen += AppendData
		m.FilePointer = start + lens
	}
	return nil

}

func (m *Mmap) ReadInt64(start int64) int64 {
	return int64(binary.LittleEndian.Uint64(m.MmapBytes[start : start+8]))
}

func (m *Mmap) ReadUInt64(start uint64) uint64 {
	return binary.LittleEndian.Uint64(m.MmapBytes[start : start+8])
}

func (m *Mmap) Read(start, end int64) []byte {

	return m.MmapBytes[start:end]
}

func (m *Mmap) Write(start int64, buffer []byte) error {

	copy(m.MmapBytes[start:int(start)+len(buffer)], buffer)

	return nil
}

func (m *Mmap) WriteUInt64(start int64, value uint64) error {
	binary.LittleEndian.PutUint64(m.MmapBytes[start:start+8], uint64(value))
	return nil
}

func (m *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(m.MmapBytes[start:start+8], uint64(value))
	return nil //m.Sync()
}

func (m *Mmap) AppendInt64(value int64) error {
	if err := m.checkFilePointer(8); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(m.MmapBytes[m.FilePointer:m.FilePointer+8], uint64(value))
	m.FilePointer += 8
	m.WriteInt64(0, m.FilePointer)
	return nil //m.Sync()
}

func (m *Mmap) AppendUInt64(value uint64) error {
	if err := m.checkFilePointer(8); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(m.MmapBytes[m.FilePointer:m.FilePointer+8], value)
	m.FilePointer += 8
	m.WriteInt64(0, m.FilePointer)
	return nil //m.Sync()
}

func (m *Mmap) AppendString(value string) error {
	lens := int64(len(value))
	if err := m.checkFilePointer(lens); err != nil {
		return err
	}
	dst := m.MmapBytes[m.FilePointer : m.FilePointer+lens]
	copy(dst, []byte(value))
	m.FilePointer += lens
	m.WriteInt64(0, m.FilePointer)
	return nil
}

func (m *Mmap) AppendStringWithLen(value string) error {
	err := m.AppendInt64(int64(len(value)))
	if err != nil {
		return err
	}
	err = m.AppendString(value)
	if err != nil {
		return err
	}
	return nil //m.Sync()
}

func (m *Mmap) AppendBytes(value []byte) error {
	lens := int64(len(value))
	if err := m.checkFilePointer(lens); err != nil {
		return err
	}
	dst := m.MmapBytes[m.FilePointer : m.FilePointer+lens]
	copy(dst, value)
	m.FilePointer += lens
	m.WriteInt64(0, m.FilePointer)
	return nil //m.Sync()

}

func (m *Mmap) WriteBytes(start int64, value []byte) error {
	lens := int64(len(value))
	dst := m.MmapBytes[start : start+lens]
	copy(dst, value)
	return nil
}

func (m *Mmap) Unmap() error {
	err := syscall.Munmap(m.MmapBytes)
	if err != nil {
		return err
	}
	err = m.FileFd.Close()
	if err != nil {
		return err
	}
	return nil
}

func (m *Mmap) Sync() error {
	// 在此处添加以下代码以确保同步整个映射的内存区域
	err := syscall.Munmap(m.MmapBytes)
	if err != nil {
		return err
	}
	// 重新映射文件到内存，以确保数据被同步到磁盘
	m.MmapBytes, err = syscall.Mmap(int(m.FileFd.Fd()), 0, int(m.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	return nil
}
