package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/types"
	"github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
	"sync"
)

// Index 索引类
type Index struct {
	Name              string            `json:"name"`
	PathName          string            `json:"pathName"`
	Fields            map[string]uint64 `json:"fields"`
	PrimaryKey        string            `json:"primaryKey"`
	StartDocId        uint64            `json:"startDocId"`
	MaxDocId          uint64            `json:"maxDocId"`
	DelDocNum         int               `json:"delDocNum"`
	NextSegmentSuffix uint64            `json:"nextSegmentSuffix"`
	SegmentNames      []string          `json:"segmentNames"`
	tempSegmentName   map[string]int
	segments          []*segment.Segment
	memorySegment     *segment.Segment
	primary           *tree.BTreeDB
	bitmap            *roaring64.Bitmap
	segmentMutex      *sync.Mutex
	Logger            *utils.Log `json:"-"`
}

// NewEmptyIndex
// @Description 创建新索引
// @Param name
// @Param pathname
// @Return
func NewEmptyIndex(name, pathname string, logger *utils.Log) *Index {
	idx := &Index{
		Name:              name,
		PathName:          pathname,
		Fields:            make(map[string]uint64),
		PrimaryKey:        "",
		StartDocId:        0,
		MaxDocId:          0,
		NextSegmentSuffix: 0,
		SegmentNames:      make([]string, 0),
		segments:          make([]*segment.Segment, 0),
		segmentMutex:      new(sync.Mutex),
		tempSegmentName:   make(map[string]int),
		Logger:            logger,
	}
	idx.bitmap = roaring64.NewBitmap()
	return idx
}

// NewIndexFromLocalFile
// @Description 反序列化索引
// @Param name 索引名
// @Param pathname 索引的存储路径
// @Return 返回索引
func NewIndexFromLocalFile(name, pathname string, logger *utils.Log) *Index {
	idx := &Index{
		Name:         name,
		PathName:     pathname,
		Fields:       make(map[string]uint64),
		SegmentNames: make([]string, 0),
		segments:     make([]*segment.Segment, 0),
		segmentMutex: new(sync.Mutex),
		Logger:       logger,
	}
	metaFileName := fmt.Sprintf("%v%v.meta", pathname, name)
	buffer, err := utils.ReadFromJson(metaFileName)
	if err != nil {
		return idx
	}
	err = json.Unmarshal(buffer, &idx)
	if err != nil {
		return idx
	}
	idx.tempSegmentName = make(map[string]int, 0)

	for index, segmentName := range idx.SegmentNames {
		idx.tempSegmentName[segmentName] = index
		seg := segment.NewSegmentFromLocalFile(segmentName, false, idx.Logger)
		idx.segments = append(idx.segments, seg)
	}
	if len(idx.segments) > 0 {
		oldSegmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix-1)
		flag := idx.segments[idx.tempSegmentName[oldSegmentName]].MaxDocId - idx.segments[idx.tempSegmentName[oldSegmentName]].StartDocId
		if flag < utils.MAX_SEGMENT_SIZE {
			idx.segments[idx.tempSegmentName[oldSegmentName]].Close()
			idx.segments[idx.tempSegmentName[oldSegmentName]] = segment.NewSegmentFromLocalFile(oldSegmentName, true, idx.Logger)
			idx.segments[idx.tempSegmentName[oldSegmentName]].ReLoadSegment()
			idx.memorySegment = idx.segments[idx.tempSegmentName[oldSegmentName]]
		} else {
			segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
			fields := make(map[string]uint64)
			for fieldName, fieldType := range idx.Fields {
				if fieldType != utils.IDX_TYPE_PK {
					fields[fieldName] = fieldType
				}
			}
			idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
			idx.NextSegmentSuffix++
		}
	} else {
		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fieldName, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fieldName] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
	}

	bitmapName := fmt.Sprintf("%v%v.bitmap", pathname, idx.Name)
	idx.bitmap, _ = utils.ReadBitMap(bitmapName)
	if idx.PrimaryKey != "" {
		primaryName := fmt.Sprintf("%v%v_primary.pk", idx.PathName, idx.Name)
		idx.primary = tree.NewBTreeDB(primaryName)
	}
	return idx
}

// AddField
// @Description 索引新增字段
// @Param field  新增的字段描述信息
// @Return 任何error
func (idx *Index) AddField(field segment.SimpleFieldInfo) error {
	if _, ok := idx.Fields[field.FieldName]; ok {
		return fmt.Errorf("[INFO] Load Index %v success", idx.Name)
	}
	idx.Fields[field.FieldName] = field.FieldType
	if field.FieldType == utils.IDX_TYPE_PK {
		idx.PrimaryKey = field.FieldName
		primaryBtree := fmt.Sprintf("%v%v_primary.pk", idx.PathName, idx.Name)
		idx.primary = tree.NewBTreeDB(primaryBtree)
		idx.primary.AddBTree(field.FieldName)
	} else {
		idx.segmentMutex.Lock()
		defer idx.segmentMutex.Unlock()
		if idx.memorySegment == nil {
			// 如果内存段为 nil 则新建一个内存段并添加字段
			segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
			fields := make(map[string]uint64)
			for fieldName, fieldType := range idx.Fields {
				if fieldType != utils.IDX_TYPE_PK {
					fields[fieldName] = fieldType
				}
			}
			idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
			idx.NextSegmentSuffix++
		} else if idx.memorySegment.IsEmpty() {
			// 如果内存段大小为0，则直接添加字段
			err := idx.memorySegment.AddField(field)
			if err != nil {
				return err
			}
		} else {
			// 如果内存段不为空，则序列化内存段，重新创建一个内存段，这个新的内存段有新增的属性
			tempSegment := idx.memorySegment
			if err := tempSegment.Serialization(); err != nil {
				return err
			}
			idx.segments = append(idx.segments, tempSegment)
			idx.SegmentNames = append(idx.SegmentNames, tempSegment.SegmentName)

			segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
			fields := make(map[string]uint64)
			for fieldName, fieldType := range idx.Fields {
				if fieldType != utils.IDX_TYPE_PK {
					fields[fieldName] = fieldType
				}
			}
			idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
			idx.NextSegmentSuffix++
		}
	}
	return idx.storeIndex()
}

func (idx *Index) SetFields(fields []segment.SimpleFieldInfo) {
	for _, field := range fields {
		idx.Fields[field.FieldName] = field.FieldType
		if field.FieldType == utils.IDX_TYPE_PK {
			idx.PrimaryKey = field.FieldName
		}
	}
	if idx.PrimaryKey != "" {
		primaryName := fmt.Sprintf("%v%v_primary.pk", idx.PathName, idx.Name)
		idx.primary = tree.NewBTreeDB(primaryName)
		idx.primary.AddBTree(idx.PrimaryKey)
	}

}

// 内部方法
func (idx *Index) storeIndex() error {
	metaFileName := fmt.Sprintf("%v%v.meta", idx.PathName, idx.Name)
	if err := utils.WriteToJson(idx, metaFileName); err != nil {
		return err
	}
	bitmapName := fmt.Sprintf("%v%v.bitmap", idx.PathName, idx.Name)
	err := utils.WriteBitMap(bitmapName, idx.bitmap)
	if err != nil {
		return err
	}
	return nil
}

// DeleteField
// @Description: 删除索引中的某个字段
// @Param fieldName 要删除的字段名
// @Return error 任何错误
func (idx *Index) DeleteField(fieldName string) error {
	if _, ok := idx.Fields[fieldName]; !ok {
		return fmt.Errorf("[WARN] Field Not Found : %v", fieldName)
	}
	if fieldName == idx.PrimaryKey {
		return fmt.Errorf("[WARN] PrimaryKey Can't Delete : %v", fieldName)
	}
	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()
	delete(idx.Fields, fieldName)

	if idx.memorySegment == nil {
		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fn, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fn] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
	} else if idx.memorySegment.IsEmpty() {
		err := idx.memorySegment.DeleteField(fieldName)
		if err != nil {
			return fmt.Errorf("[ERROR] Delete Field Error : %v", err)
		}
	} else {
		tempSegment := idx.memorySegment
		if err := tempSegment.Serialization(); err != nil {
			return err
		}
		idx.segments = append(idx.segments, tempSegment)
		idx.SegmentNames = append(idx.SegmentNames, tempSegment.SegmentName)

		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fn, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fn] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
	}

	return idx.storeIndex()
}

// AddDocument
// @Description: 新增文档
// @Param content 一个map，key是字段，value是内容
// @Return uint32 文档Id
// @Return error 任何error
func (idx *Index) AddDocument(doc *doc.Document) (uint64, error) {
	_, ok := idx.findPrimaryKey(doc.Id)
	if ok {
		return 0, errors.New("document has exits")
	}
	if len(idx.Fields) == 0 {
		return 0, errors.New("index has no Field")
	}
	// 在段内文档数到达阈值时进行持久化
	if idx.memorySegment != nil && idx.memorySegment.MaxDocId-idx.memorySegment.StartDocId >= utils.MAX_SEGMENT_SIZE {
		err := idx.SyncMemorySegment()
		if err != nil {
			return 0, err
		}
	}
	if idx.memorySegment == nil {
		idx.segmentMutex.Lock()
		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fieldName, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK && fieldType != utils.IDX_TYPE_DESC {
				fields[fieldName] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
		if err := idx.storeIndex(); err != nil {
			idx.segmentMutex.Unlock()
			return 0, err
		}
		idx.segmentMutex.Unlock()
	}
	docId := idx.MaxDocId
	idx.MaxDocId++
	if idx.PrimaryKey != "" {
		idx.primary.Set(idx.PrimaryKey, PrimaryKey(doc.Id), docId)
	}
	return docId, idx.memorySegment.AddDocument(docId, doc)
}

// GetDocument
// @Description: 根据文档ID获取文档内容
// @Param docId 文档ID
// @Return map[string]string 文档内容，key是字段名，value是内容
func (idx *Index) GetDocument(docId uint64) (*doc.Document, bool) {
	segIndex := uint64((docId - 1) / utils.MAX_SEGMENT_SIZE)
	segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, segIndex)
	if len(idx.segments) == 0 {
		idx.Logger.NFLog.Warningf("segment [%v] no has exsits", segmentName)
		return nil, false
	}
	seg := idx.segments[idx.tempSegmentName[segmentName]]
	if docId >= seg.StartDocId && docId < seg.MaxDocId {
		return seg.GetDocument(docId)
	}
	return idx.memorySegment.GetDocument(docId)
}

func (idx *Index) IsNotDelete(primaryKey string) (uint64, bool) {
	docId, exits := idx.findPrimaryKey(primaryKey)
	if exits && !idx.bitmap.Contains(docId) {
		return docId, true
	}
	return 0, false
}

// SyncMemorySegment
// @Description 内存段序列化
// @Return 任何error
func (idx *Index) SyncMemorySegment() error {
	if idx.memorySegment == nil {
		return idx.storeIndex()
	}
	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()
	if idx.memorySegment.IsEmpty() {
		return nil
	}
	if err := idx.memorySegment.Serialization(); err != nil {
		return err
	}
	segmentName := idx.memorySegment.SegmentName

	err := idx.memorySegment.Close()
	if err != nil {
		return err
	}
	idx.memorySegment = nil
	newSegment := segment.NewSegmentFromLocalFile(segmentName, false, idx.Logger)
	if _, ok := idx.tempSegmentName[segmentName]; ok {
		idx.segments[idx.tempSegmentName[segmentName]] = newSegment
	} else {
		idx.segments = append(idx.segments, newSegment)
		idx.tempSegmentName[segmentName] = len(idx.segments) - 1
		idx.SegmentNames = append(idx.SegmentNames, segmentName)
	}
	return idx.storeIndex()
}

// DeleteDocument
// @Description: 根据主键删除文档
// @param primaryKey 根据
// @return error 任何错误
func (idx *Index) DeleteDocument(primaryKey string) error {
	docId, ok := idx.findPrimaryKey(primaryKey)
	if ok {
		if idx.bitmap.Contains(docId) {
			return fmt.Errorf("index has no document")
		}
		idx.bitmap.Add(docId)
		idx.DelDocNum++
		return nil
	}
	return nil
}

type PrimaryKey string

func (pk PrimaryKey) ToBytes() []byte {
	return []byte(pk)
}

func (idx *Index) findPrimaryKey(primaryKey string) (uint64, bool) {
	if idx.PrimaryKey == "" {
		return 0, false
	}
	ok, docId := idx.primary.Search(idx.PrimaryKey, PrimaryKey(primaryKey))
	if !ok {
		return 0, false
	}
	return docId, true
}

func (idx *Index) Search(query *types.TermQuery, filters []*types.SearchFilters) []*doc.Document {
	docList := make([]*doc.Document, 0)
	for _, segIndex := range idx.tempSegmentName {

		temp := idx.segments[segIndex].Search(query, filters, idx.bitmap)
		if len(temp) > 0 {
			docList = append(docList, temp...)
		}
	}
	return docList
}

func (idx *Index) Close() error {
	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()
	// Close memory segment
	idx.memorySegment = nil
	// Close segments
	for _, seg := range idx.segments {
		if err := seg.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %v", err)
		}
	}
	idx.segments = nil

	idx.tempSegmentName = nil

	idx.bitmap = nil

	idx.primary = nil
	return nil
}
