/*****************************************************************************
 *  file name : profile.go
 *  author : cyl
 *  email  : 2842871262@qq.com
 *  file description : 正排索引
 *
******************************************************************************/

package segment

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
)

type DocId uint64

func (n DocId) ToBytes() []byte {
	return utils.ItoBytes(n)
}

type Profile struct {
	startDocId    uint64
	maxDocId      uint64
	segmentName   string
	memoryHashMap map[DocId][]byte
	isMemory      bool
	dtlMmap       *utils.Mmap
	btree         *tree.BTreeDB
	logger        *utils.Log
}

func NewEmptyProfile(segmentName string, start uint64, logger *utils.Log) *Profile {
	this := &Profile{segmentName: segmentName, startDocId: start, maxDocId: start, isMemory: true, logger: logger}
	this.memoryHashMap = make(map[DocId][]byte)
	return this
}

func NewProfile(segmentName string, start, max uint64, dtlMmap *utils.Mmap, btree *tree.BTreeDB, logger *utils.Log) *Profile {

	this := &Profile{segmentName: segmentName, startDocId: start, maxDocId: max, dtlMmap: dtlMmap, btree: btree, isMemory: false, logger: logger}
	return this
}

func (pfl *Profile) setDtlMmap(dtlMmap *utils.Mmap) {
	pfl.dtlMmap = dtlMmap
}

func (pfl *Profile) setBtree(btree *tree.BTreeDB) {
	pfl.btree = btree
}

func (pfl *Profile) AddDocument(docId uint64, d *doc.Document) error {
	if docId != pfl.maxDocId {
		return errors.New("profile AddDocument :: Wrong DocId Number")
	}
	var buf bytes.Buffer
	// 创建一个编码器，并将其连接到字节缓冲区
	enc := gob.NewEncoder(&buf)
	// 使用编码器对 MyData 结构体进行序列化
	enc.Encode(d)
	if pfl.memoryHashMap == nil {
		pfl.memoryHashMap = make(map[DocId][]byte)
	}
	pfl.memoryHashMap[DocId(docId)] = buf.Bytes()
	pfl.maxDocId++
	return nil
}

func (pfl *Profile) Serialization(segmentName string, btree *tree.BTreeDB) error {
	btName := fmt.Sprintf("%v_profile", segmentName) //索引树名
	dataFileName := fmt.Sprintf("%v_profile.dtl", segmentName)
	mmap, err := utils.NewMmap(dataFileName, 0)
	if err != nil {
		return err
	}
	var nowOffset uint64
	nowOffset = uint64(mmap.FilePointer)
	defer mmap.Unmap()
	tx, err := btree.BeginTx()
	if err != nil {
		return err
	}
	b, err := tx.CreateBucketIfNotExists([]byte(btName))
	for key, value := range pfl.memoryHashMap {
		mmap.AppendUInt64(uint64(len(value)))
		mmap.AppendBytes(value)
		btree.Put(b, key, nowOffset)
		nowOffset = uint64(mmap.FilePointer)
	}
	btree.Commit(tx)
	pfl.memoryHashMap = nil
	pfl.isMemory = false
	return nil
}

func (pfl *Profile) Query(docId uint64) (*doc.Document, bool) {
	btName := fmt.Sprintf("%v_profile", pfl.segmentName)
	if pfl.memoryHashMap != nil && pfl.isMemory {
		if value, exists := pfl.memoryHashMap[DocId(docId)]; exists {
			reader := bytes.NewReader(value)
			decoder := gob.NewDecoder(reader)
			var document doc.Document
			err := decoder.Decode(&document)
			if err != nil {
				return nil, false
			}
			return nil, true
		} else {
			return nil, false
		}
	}
	if docId >= pfl.startDocId && pfl.dtlMmap != nil &&
		pfl.maxDocId > docId {
		_, offset := pfl.btree.Search(btName, DocId(docId))
		lenBuffer := pfl.dtlMmap.ReadUInt64(offset)
		buffer := pfl.dtlMmap.Read(int64(offset+8), int64(offset+8+lenBuffer))
		reader := bytes.NewReader(buffer)
		decoder := gob.NewDecoder(reader)
		var document doc.Document
		err := decoder.Decode(&document)
		if err != nil {
			return nil, false
		}
		return &document, true
	}
	return nil, false
}

func (pfl *Profile) UpDocument(docId uint64, d *doc.Document) error {
	if pfl.isMemory {
		return pfl.AddDocument(docId, d)
	} else {
		btName := fmt.Sprintf("%v_profile", pfl.segmentName)
		exits := pfl.btree.HasKey(btName, DocId(docId))
		if exits {
			var buf bytes.Buffer
			// 创建一个编码器，并将其连接到字节缓冲区
			enc := gob.NewEncoder(&buf)
			// 使用编码器对 MyData 结构体进行序列化
			enc.Encode(d)
			var nowOffset uint64
			nowOffset = uint64(pfl.dtlMmap.FilePointer)

			pfl.dtlMmap.AppendUInt64(uint64(len(buf.Bytes())))
			pfl.dtlMmap.AppendBytes(buf.Bytes())
			pfl.btree.Set(btName, DocId(docId), nowOffset)
			pfl.SyncProfile()
		}
	}
	return nil
}

func (pfl *Profile) SyncProfile() error {
	err := pfl.dtlMmap.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (pfl *Profile) Close() {
	if pfl.dtlMmap != nil {
		pfl.dtlMmap.Unmap()
	}
	pfl.memoryHashMap = nil
}
