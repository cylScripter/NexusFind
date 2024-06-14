package segment

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/types"
	"github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
	"os"
)

type Segment struct {
	StartDocId  uint64            `json:"startDocId"`  // 段内docId的最小值
	MaxDocId    uint64            `json:"maxDocId"`    // 段内docId的最大值
	SegmentName string            `json:"segmentName"` // 段的名称，序列化时文件名的一部分
	FieldInfos  map[string]uint64 `json:"fields"`      // 记录段内字段的类型信息
	fields      map[string]*Field // 段内字段的
	pfl         *Profile
	isMemory    bool          // 标识段是否在内存中
	btdb        *tree.BTreeDB // 段的数据库
	Logger      *utils.Log    `json:"-"`
}

// NewEmptySegmentByFieldsInfo
// @Description 根据字段信息创建段
// @Param segmentName  段名
// @Param start  文档起始Id
// @Param fields  字段信息
// @Return 新建的段
func NewEmptySegmentByFieldsInfo(segmentName string, start uint64, fields map[string]uint64, logger *utils.Log) *Segment {
	seg := &Segment{
		StartDocId:  start,
		MaxDocId:    start,
		SegmentName: segmentName,
		FieldInfos:  fields,
		fields:      make(map[string]*Field),
		isMemory:    true,
		btdb:        nil,
		Logger:      logger,
	}
	for fieldName, fieldType := range fields {
		f := NewEmptyField(fieldName, start, fieldType, seg.Logger)
		seg.fields[fieldName] = f
	}
	seg.pfl = NewEmptyProfile(segmentName, start, seg.Logger)
	return seg
}

func (seg *Segment) SetMemory() {
	seg.isMemory = true
}

func (seg *Segment) Get() any {
	return seg.isMemory
}

// NewSegmentFromLocalFile
// @Description 反序列化段
// @Param segmentName  段名
// @Return 反序列化的段
func NewSegmentFromLocalFile(segmentName string, flag bool, logger *utils.Log) *Segment {
	seg := &Segment{
		StartDocId:  0,
		MaxDocId:    0,
		SegmentName: segmentName,
		FieldInfos:  make(map[string]uint64),
		fields:      make(map[string]*Field),
		isMemory:    false,
		btdb:        nil,
		Logger:      logger,
	}
	metaFileName := fmt.Sprintf("%v%v", segmentName, "seg.meta")
	buf, err := utils.ReadFromJson(metaFileName)
	if err != nil {
		return seg
	}
	err = json.Unmarshal(buf, &seg)
	if err != nil {
		return seg
	}

	btdbName := fmt.Sprintf("%v%v", segmentName, "seg.bt")
	if utils.Exist(btdbName) {
		seg.btdb = tree.NewBTreeDB(btdbName)
	}
	for name := range seg.FieldInfos {
		nowField := NewFieldFromLocalFile(name, segmentName, seg.StartDocId, seg.MaxDocId, seg.FieldInfos[name], seg.btdb, flag, seg.Logger)
		seg.fields[name] = nowField
	}
	mmap, err := utils.NewMmap(fmt.Sprintf("%v_profile.dtl", segmentName), utils.ModeAppend)
	if err != nil {
		fmt.Printf("[ERROR] Mmap error : %v\n", err)
		return seg
	}
	seg.pfl = NewProfile(segmentName, seg.StartDocId, seg.MaxDocId, mmap, seg.btdb, seg.Logger)
	return seg
}

func (seg *Segment) ReLoadSegment() error {
	for i := seg.StartDocId; i < seg.MaxDocId; i++ {
		document, exits := seg.GetDocument(i)
		if exits {
			for name, _ := range seg.fields {
				if _, ok := document.Content[name]; ok {
					if err := seg.fields[name].AddDocument(i, document.Content[name]); err != nil {
						fmt.Printf("[ERROR] Segment AddDocument :: field[%v] value[%v] error[%v]\n", name, document.Content[name], err)
					}
				}
			}
		}
	}
	seg.SetMemory()
	return nil
}

// AddField
// @Description 添加字段
// @Param newField  字段信息
// @Return 任何错误
func (seg *Segment) AddField(newField SimpleFieldInfo) error {
	if _, ok := seg.FieldInfos[newField.FieldName]; ok {
		return errors.New("segment has field")
	}
	if seg.isMemory && !seg.IsEmpty() {
		return errors.New("segment can't add field")
	}
	f := NewEmptyField(newField.FieldName, seg.StartDocId, newField.FieldType, seg.Logger)
	seg.FieldInfos[newField.FieldName] = newField.FieldType
	seg.fields[newField.FieldName] = f
	return nil
}

// IsEmpty
// @Description 判断是否是空段
// @Return 如果是空段就返回 true
func (seg *Segment) IsEmpty() bool {
	return seg.StartDocId == seg.MaxDocId
}

// DeleteField
// @Description 删除字段
// @Param fieldName 字段名
// @Return error 任何error
func (seg *Segment) DeleteField(fieldName string) error {
	// 如果段中没有需要删除的字段
	if _, ok := seg.FieldInfos[fieldName]; !ok {
		return errors.New("segment doesn't has field")
	}
	// 如果段已经存在，并且段中有数据，则段不可更改
	if seg.isMemory && !seg.IsEmpty() {
		return errors.New("segment can't delete field")
	}
	seg.fields[fieldName].destroy()
	delete(seg.FieldInfos, fieldName)
	delete(seg.fields, fieldName)
	return nil
}

// AddDocument
// @Description    为段里的 Field 新增文档
// @Param docId    文档ID
// @Param content  map[字段名]内容
// @Return error   任何错误
func (seg *Segment) AddDocument(docId uint64, d *doc.Document) error {
	if docId != seg.MaxDocId {
		return errors.New("segment Maximum ID Mismatch")
	}
	for name, _ := range seg.fields {
		if _, ok := d.Content[name]; ok {
			if err := seg.fields[name].AddDocument(docId, d.Content[name]); err != nil {
				fmt.Printf("[ERROR] Segment AddDocument :: field[%v] value[%v] error[%v]\n", name, d.Content[name], err)
			}
		}
	}
	err := seg.pfl.AddDocument(docId, d)
	if err != nil {
		return err
	}
	seg.MaxDocId++
	return nil
}

// Serialization
// @Description 序列化段
// @Return 任何error
func (seg *Segment) Serialization() error {
	err := os.MkdirAll(fmt.Sprintf(seg.SegmentName), 0755)
	if err != nil {
		return err
	}
	btdbName := fmt.Sprintf("%v%v", seg.SegmentName, "seg.bt")
	if seg.btdb == nil {
		seg.btdb = tree.NewBTreeDB(btdbName)
	}
	for fieldName := range seg.FieldInfos {
		if err := seg.fields[fieldName].Serialization(seg.SegmentName, seg.btdb); err != nil {
			return err
		}
	}
	err = seg.pfl.Serialization(seg.SegmentName, seg.btdb)
	if err != nil {
		return err
	}
	if err := seg.storeSegment(); err != nil {
		return err
	}
	seg.isMemory = false
	return nil
}

// 内部方法
func (seg *Segment) storeSegment() error {
	metaFileName := fmt.Sprintf("%v%v.meta", seg.SegmentName, "seg")
	if err := utils.WriteToJson(seg, metaFileName); err != nil {
		return err
	}
	return nil
}

// GetDocument
// @Description 根据 docId 获取文档内容
// @Param docId 文档ID
// @Return map[string]string 返回的内容
// @Return bool 是否找到文档
func (seg *Segment) GetDocument(docId uint64) (*doc.Document, bool) {
	if docId < seg.StartDocId || docId >= seg.MaxDocId {
		return nil, false
	}
	return seg.pfl.Query(docId)
}

// Close
// @Description 将段从内存中回收
// @Return 任何error
func (seg *Segment) Close() error {
	for _, field := range seg.fields {
		field.destroy()
	}
	if seg.btdb != nil {
		err := seg.btdb.Close()
		if err != nil {
			return err
		}
	}
	seg.pfl.Close()
	return nil
}

// Destroy
// @Description 将段从磁盘中移除
// @Return 任何error
func (seg *Segment) Destroy() error {
	err := seg.Close()
	if err != nil {
		return err
	}
	dirName := fmt.Sprintf("%v", seg.SegmentName)
	err = os.RemoveAll(dirName)
	if err != nil {
		return err
	}
	return nil
}

func (seg *Segment) searchFilter(filters []*types.SearchFilters) (*roaring64.Bitmap, bool) {
	if filters == nil || len(filters) == 0 {
		return nil, false
	}
	// Check if the first filter's field is present
	firstField, ok := seg.fields[filters[0].FieldName]
	if !ok {
		return nil, false
	}
	// Apply the first filter
	firstBitmap, exits := firstField.QueryFilter(filters[0])
	if !exits {
		return nil, false
	}
	// Apply remaining filters
	for _, filter := range filters[1:] {
		field, ok := seg.fields[filter.FieldName]
		if !ok {
			return nil, false
		}
		tempBitmap, exits := field.QueryFilter(filter)
		if !exits {
			return nil, false
		}
		firstBitmap.And(tempBitmap)
	}

	return firstBitmap, true
}

func (seg *Segment) search(query *types.TermQuery) *roaring64.Bitmap {

	if query.Keyword != nil {
		bitMap, exits := seg.fields[query.Keyword.Field].Query(query.Keyword.Word)
		if exits {
			return bitMap
		}
		return roaring64.NewBitmap()
	} else if len(query.Must) > 0 {
		results := make([]*roaring64.Bitmap, 0, len(query.Must))
		for _, q := range query.Must {
			results = append(results, seg.search(q))
		}
		return types.IntersectBitmaps(results)
	} else if len(query.Should) > 0 {
		results := make([]*roaring64.Bitmap, 0, len(query.Should))
		for _, q := range query.Should {
			results = append(results, seg.search(q))
		}
		return types.UnionBitmaps(results)
	}
	return roaring64.NewBitmap()
}

func (seg *Segment) Search(query *types.TermQuery, filters []*types.SearchFilters, deleteBitmap *roaring64.Bitmap) []*doc.Document {
	docList := make([]*doc.Document, 0)
	result := seg.search(query)

	filterResult, exits := seg.searchFilter(filters)

	if exits {
		result.And(filterResult)
	}
	docIds := result.ToArray()
	for _, docId := range docIds {
		document, exits := seg.GetDocument(docId)
		if exits && !deleteBitmap.Contains(docId) {
			docList = append(docList, document)
		}
	}
	return docList
}

func (seg *Segment) SearchDocId(query *types.TermQuery, filters []*types.SearchFilters, deleteBitmap *roaring64.Bitmap) []uint64 {
	docIds := make([]uint64, 0)
	result := seg.search(query)
	filterResult, exits := seg.searchFilter(filters)
	if exits {
		result.And(filterResult)
	}
	for _, docId := range result.ToArray() {
		if deleteBitmap.Contains(docId) {
			docIds = append(docIds, docId)
		}
	}
	return docIds
}
