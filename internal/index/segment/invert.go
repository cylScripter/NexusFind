/*****************************************************************************
 *  file name : invert.go
 *  author : cyl
 *  email  : 2842871262@qq.com
 *  file description : 倒排索引
 *
******************************************************************************/

package segment

import (
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/utils"
	"strconv"
)

type Term string

func (t Term) ToBytes() []byte {
	return []byte(t)
}

type Number int64

func (n Number) ToBytes() []byte {
	return utils.ItoBytes(n)
}

/************************************************************************

字符型倒排索引，操作文件
B+树  [fieldName]_invert  倒排索引
[segmentName][fieldName]_invert.idx 该段的倒排列表文件

************************************************************************/

// Invert 倒排索引
type invert struct {
	curDocId  uint64
	isMemory  bool
	fieldType uint64
	fieldName string
	idxMmap   *utils.Mmap   // 该段的倒排列表文件内存映射
	bti       *tree.BTreeDB // 倒排索引结构
	logger    *utils.Log
}

func newEmptyInvert(fieldType uint64, startDocId uint64, fieldName string, logger *utils.Log) *invert {
	ivt := &invert{
		curDocId:  startDocId,
		isMemory:  true,
		fieldType: fieldType,
		fieldName: fieldName,
		logger:    logger,
		bti:       nil,
	}
	return ivt
}

func newInvert(fieldType uint64, btree *tree.BTreeDB, fieldName string, idxMmap *utils.Mmap, curDocId uint64, Memory bool, logger *utils.Log) *invert {
	ivt := &invert{
		isMemory:  Memory,
		fieldType: fieldType,
		fieldName: fieldName,
		idxMmap:   idxMmap,
		bti:       nil,
		curDocId:  curDocId,
		logger:    logger,
	}
	ivt.bti = btree
	return ivt
}

func (ivt *invert) setIdxMmap(mmap *utils.Mmap) {
	ivt.idxMmap = mmap
}
func (ivt *invert) SetMemory() {
	ivt.isMemory = true
}
func (ivt *invert) setBti(btree *tree.BTreeDB) {
	ivt.bti = btree
}
func (ivt *invert) close() {
	if ivt.idxMmap != nil {
		ivt.idxMmap.Unmap()
	}
}
func (ivt *invert) setFiled(filedName string, filedType uint64) {
	ivt.fieldName = filedName
	ivt.fieldType = filedType
}

// TextInvert 文本类型倒排索引
type TextInvert struct {
	*invert
	memoryHashMap map[Term]*roaring64.Bitmap //key为词项，value为用位图保存倒排列表
}

// NumberInvert 数值类型倒排索引
type NumberInvert struct {
	*invert
	memoryHashMap map[Number]*roaring64.Bitmap //key为词项，value为用位图保存倒排列表
}

func NewEmptyTextInvert(fieldType uint64, startDocId uint64, fieldName string, logger *utils.Log) *TextInvert {
	ivt := newEmptyInvert(fieldType, startDocId, fieldName, logger)
	return &TextInvert{
		ivt,
		nil,
	}
}

func NewTextInvert(fieldType uint64, btree *tree.BTreeDB, fieldName string, idxMmap *utils.Mmap, curDocId uint64, Memory bool, logger *utils.Log) *TextInvert {
	ivt := newInvert(fieldType, btree, fieldName, idxMmap, curDocId, Memory, logger)
	return &TextInvert{
		invert:        ivt,
		memoryHashMap: nil,
	}
}

func (ivt *TextInvert) destroy() {
	ivt.memoryHashMap = nil
}

func (ivt *TextInvert) AddDocument(docId uint64, contentStr string) error {
	if docId != ivt.curDocId {
		return errors.New("text invert AddDocument :: Wrong DocId Number")
	}
	var segResult []string
	if ivt.fieldType == utils.IDX_TYPE_STRING {
		segResult = []string{contentStr}
	} else if ivt.fieldType == utils.IDX_TYPE_STRING_SEG {
		segmented := utils.GetGseSegmenter()
		segResult = segmented.CutSearch(contentStr, false)
	} else {
		return errors.New("invert fieldType is not exists")
	}
	if ivt.memoryHashMap == nil {
		ivt.memoryHashMap = make(map[Term]*roaring64.Bitmap)
	}
	for _, seg := range segResult {
		if ivt.memoryHashMap[Term(seg)] == nil {
			ivt.memoryHashMap[Term(seg)] = roaring64.New()
		}
		ivt.memoryHashMap[Term(seg)].Add(docId)
	}
	ivt.curDocId++
	return nil
}

func (ivt *TextInvert) Serialization(segmentName string, btree *tree.BTreeDB) error {
	btName := fmt.Sprintf("%v_invert", ivt.fieldName) //索引树名
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	mmap, err := utils.NewMmap(idxFileName, utils.ModeCreate)
	if err != nil {
		return err
	}
	var nowOffset uint64
	nowOffset = uint64(mmap.FilePointer)
	defer mmap.Unmap()
	err = btree.AddBTree(btName)
	if err != nil {
		fmt.Println(err)
	}
	tx, err := btree.BeginTx()
	if err != nil {
		return err
	}
	b, err := tx.CreateBucketIfNotExists([]byte(btName))
	for key, value := range ivt.memoryHashMap {
		bits, _ := value.ToBytes()
		mmap.AppendUInt64(uint64(len(bits)))
		mmap.AppendBytes(bits)
		btree.Put(b, key, nowOffset)
		nowOffset += uint64(len(bits)) + 8
	}
	btree.Commit(tx)
	ivt.memoryHashMap = nil
	ivt.isMemory = false
	return nil
}

func (ivt *TextInvert) QueryTerm(keyStr string) (*roaring64.Bitmap, bool) {
	btName := fmt.Sprintf("%v_invert", ivt.fieldName)
	if ivt.isMemory == true {
		if _, ok := ivt.memoryHashMap[Term(keyStr)]; ok {
			return ivt.memoryHashMap[Term(keyStr)], true
		}
		return nil, false
	} else if ivt.idxMmap != nil {
		idxBitMap := roaring64.New()
		exits, offset := ivt.bti.Search(btName, Term(keyStr))
		if exits {
			lenBuffer := ivt.idxMmap.ReadUInt64(offset)
			bits := ivt.idxMmap.MmapBytes[offset+8 : offset+8+lenBuffer]

			err := idxBitMap.UnmarshalBinary(bits)
			if err != nil {
				fmt.Println(err)
			}
			return idxBitMap, true
		}
	}
	return nil, false
}

func NewEmptyNumberInvert(fieldType uint64, startDocId uint64, fieldName string, logger *utils.Log) *NumberInvert {
	ivt := newEmptyInvert(fieldType, startDocId, fieldName, logger)
	return &NumberInvert{
		ivt,
		nil,
	}
}

func NewNumberInvert(fieldType uint64, btree *tree.BTreeDB, fieldName string, idxMmap *utils.Mmap, curDocId uint64, Memory bool, logger *utils.Log) *NumberInvert {
	ivt := newInvert(fieldType, btree, fieldName, idxMmap, curDocId, Memory, logger)
	return &NumberInvert{
		invert:        ivt,
		memoryHashMap: nil,
	}
}

func (ivt *NumberInvert) destroy() {
	ivt.memoryHashMap = nil
}

func (ivt *NumberInvert) AddDocument(docId uint64, contentStr string) error {
	if docId != ivt.curDocId {
		return errors.New("number index AddDocument :: Wrong DocId Number")
	}
	var value int64 = -1
	switch ivt.fieldType {
	case utils.IDX_TYPE_NUMBER:
		intValue, err := strconv.ParseInt(contentStr, 10, 64)
		if err != nil {
			intValue = -1
		}
		value = intValue
	case utils.IDX_TYPE_FLOAT:
		floatValue, err := strconv.ParseFloat(contentStr, 64)
		if err != nil {
			floatValue = -0.01
		}
		value = int64(floatValue * 100)
	case utils.IDX_TYPE_DATE:
		value, _ = utils.IsDateTime(contentStr)
	}
	if ivt.memoryHashMap == nil {
		ivt.memoryHashMap = make(map[Number]*roaring64.Bitmap)
	}
	if ivt.memoryHashMap[Number(value)] == nil {
		ivt.memoryHashMap[Number(value)] = roaring64.New()
	}
	ivt.memoryHashMap[Number(value)].Add(docId)
	ivt.curDocId++
	return nil
}

func (ivt *NumberInvert) Serialization(segmentName string, btree *tree.BTreeDB) error {
	btName := fmt.Sprintf("%v_invert", ivt.fieldName) //索引树名
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	mmap, err := utils.NewMmap(idxFileName, utils.ModeCreate)
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
	for key, value := range ivt.memoryHashMap {
		bits, _ := value.ToBytes()
		mmap.AppendUInt64(uint64(len(bits)))
		mmap.AppendBytes(bits)
		btree.Put(b, key, nowOffset)
		nowOffset += uint64(len(bits)) + 8
	}
	btree.Commit(tx)
	ivt.memoryHashMap = nil
	ivt.isMemory = false
	return nil
}

func (ivt *NumberInvert) GetNextKV(key int64) (int64, uint64, bool) {
	if ivt.bti == nil {
		return 0, 0, false
	}
	btName := fmt.Sprintf("%v_invert", ivt.fieldName)
	return ivt.bti.GetNextKV(btName, key)
}

func (ivt *NumberInvert) GetFirstKV() (int64, uint64, bool) {
	if ivt.bti == nil {
		return 0, 0, false
	}
	btName := fmt.Sprintf("%v_invert", ivt.fieldName)
	return ivt.bti.GetFirstKV(btName)
}

func (ivt *NumberInvert) QueryTerm(key int64) (*roaring64.Bitmap, bool) {
	btName := fmt.Sprintf("%v_invert", ivt.fieldName)
	fmt.Println(btName)
	if ivt.isMemory == true {
		return ivt.memoryHashMap[Number(key)], true
	} else if ivt.idxMmap != nil {
		idxBitMap := roaring64.New()
		exits, offset := ivt.bti.Search(btName, Number(key))
		if exits {
			lenBuffer := ivt.idxMmap.ReadUInt64(offset)
			bits := ivt.idxMmap.MmapBytes[offset+8 : offset+8+lenBuffer]
			err := idxBitMap.UnmarshalBinary(bits)
			if err != nil {
				fmt.Println(err)
			}
			return idxBitMap, true
		}
	}
	return nil, false
}

func (ivt *NumberInvert) QueryRange(keyMin, keyMax int64) (*roaring64.Bitmap, bool) {
	bitMap := roaring64.New()
	if ivt.isMemory == true {
		for k, v := range ivt.memoryHashMap {
			if int64(k) >= keyMin && int64(k) <= keyMax {
				bitMap.Or(v)
			}
		}
		return bitMap, true
	} else if ivt.idxMmap != nil {
		btName := fmt.Sprintf("%v_invert", ivt.fieldName)
		ok, offsets := ivt.bti.SearchRange(btName, keyMin, keyMax)
		if ok {
			for _, offset := range offsets {
				lenBuffer := ivt.idxMmap.ReadUInt64(offset)
				bits := ivt.idxMmap.MmapBytes[offset+8 : offset+8+lenBuffer]
				idxBitMap := roaring64.New()
				err := idxBitMap.UnmarshalBinary(bits)
				if err != nil {
					fmt.Println(err)
				}
				bitMap.Or(idxBitMap)
			}
			return bitMap, true
		}
	}
	return nil, false
}
