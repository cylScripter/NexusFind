package segment

import (
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/types"
	"github.com/cylScripter/NexusFind/utils"
)

type SimpleFieldInfo struct {
	FieldName string `json:"fieldName"`
	FieldType uint64 `json:"fieldType"`
}

type Field struct {
	fieldName    string
	startDocId   uint64
	maxDocId     uint64
	fieldType    uint64
	isMemory     bool
	textInvert   *TextInvert
	numberInvert *NumberInvert
	btree        *tree.BTreeDB
	logger       *utils.Log
}

func NewEmptyField(fieldName string, start, fieldType uint64, logger *utils.Log) *Field {
	f := &Field{
		fieldName:  fieldName,
		startDocId: start,
		maxDocId:   start,
		fieldType:  fieldType,
		isMemory:   true,
		logger:     logger,
	}
	if fieldType == utils.IDX_TYPE_STRING ||
		fieldType == utils.IDX_TYPE_STRING_SEG {
		f.textInvert = NewEmptyTextInvert(fieldType, start, fieldName, logger)
	}
	if fieldType == utils.IDX_TYPE_NUMBER ||
		fieldType == utils.IDX_TYPE_DATE ||
		fieldType == utils.IDX_TYPE_FLOAT {
		f.numberInvert = NewEmptyNumberInvert(fieldType, start, fieldName, logger)
	}
	return f
}

func NewFieldFromLocalFile(fieldName, segmentName string, start, max uint64, fieldType uint64, btree *tree.BTreeDB, flag bool, logger *utils.Log) *Field {
	f := &Field{
		fieldName:  fieldName,
		startDocId: start,
		maxDocId:   max,
		fieldType:  fieldType,
		isMemory:   false,
		btree:      btree,
		logger:     logger,
	}
	if max-start < utils.MAX_SEGMENT_SIZE && flag {
		f.isMemory = true
		f.maxDocId = start
	}
	mmap, err := utils.NewMmap(fmt.Sprintf("%v%v_invert.idx", segmentName, f.fieldName), utils.ModeAppend)
	if err != nil {
		fmt.Printf("[ERROR] Mmap error : %v\n", err)
	}
	if fieldType == utils.IDX_TYPE_STRING ||
		fieldType == utils.IDX_TYPE_STRING_SEG {
		f.textInvert = NewTextInvert(fieldType, btree, fieldName, mmap, f.maxDocId, f.isMemory, logger)
	}
	if fieldType == utils.IDX_TYPE_NUMBER ||
		fieldType == utils.IDX_TYPE_DATE ||
		fieldType == utils.IDX_TYPE_FLOAT {
		f.numberInvert = NewNumberInvert(fieldType, btree, fieldName, mmap, f.maxDocId, f.isMemory, logger)
	}
	return f
}

func (f *Field) SetMemory() {
	f.isMemory = true
}

func (f *Field) AddDocument(docId uint64, contentStr string) error {
	if docId != f.maxDocId || f.isMemory == false {
		return errors.New("[ERROR] Wrong docid")
	}
	if (f.fieldType == utils.IDX_TYPE_STRING_SEG ||
		f.fieldType == utils.IDX_TYPE_STRING) &&
		f.textInvert != nil {
		if err := f.textInvert.AddDocument(docId, contentStr); err != nil {
			return err
		}
	}
	if (f.fieldType == utils.IDX_TYPE_NUMBER ||
		f.fieldType == utils.IDX_TYPE_DATE ||
		f.fieldType == utils.IDX_TYPE_FLOAT) &&
		f.numberInvert != nil {
		if err := f.numberInvert.AddDocument(docId, contentStr); err != nil {
			return err
		}
	}
	f.maxDocId++
	return nil
}

func (f *Field) Query(key string) (*roaring64.Bitmap, bool) {
	if f.textInvert == nil {
		return nil, false
	}
	return f.textInvert.QueryTerm(fmt.Sprintf("%v", key))
}

func (f *Field) QueryFilter(filter *types.SearchFilters) (*roaring64.Bitmap, bool) {
	if f.numberInvert == nil {
		return nil, false
	}
	var start, end int64
	switch filter.Type {
	case utils.FILT_EQ:
		if f.fieldType == utils.IDX_TYPE_DATE {
			start = filter.Start
			end = filter.Start + 86399
			break
		} else {
			start = filter.Start
			end = filter.Start
		}
	case utils.FILT_RANGE:
		start = filter.Start
		end = filter.End
	case utils.FILT_LESS:
		start = 0
		end = filter.Start
	case utils.FILT_OVER:
		start = filter.Start
		end = 0xFFFFFFFFFF
	}
	return f.numberInvert.QueryRange(start, end)
}

func (f *Field) Serialization(segmentName string, db *tree.BTreeDB) error {
	f.btree = db
	if (f.fieldType == utils.IDX_TYPE_STRING_SEG ||
		f.fieldType == utils.IDX_TYPE_STRING) &&
		f.textInvert != nil {
		f.textInvert.setBti(db)
		err := f.textInvert.Serialization(segmentName, db)
		if err != nil {
			return err
		}
	}
	if (f.fieldType == utils.IDX_TYPE_NUMBER ||
		f.fieldType == utils.IDX_TYPE_DATE ||
		f.fieldType == utils.IDX_TYPE_FLOAT) &&
		f.numberInvert != nil {
		f.numberInvert.setBti(db)
		err := f.numberInvert.Serialization(segmentName, db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Field) destroy() {
	if f.textInvert != nil {
		f.textInvert.destroy()
		f.textInvert.close()
	}
	if f.numberInvert != nil {
		f.numberInvert.destroy()
		f.numberInvert.close()
	}
}
