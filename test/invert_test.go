package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/utils"
)

var segmentName = "s1"

var textFieldInfo = &segment.SimpleFieldInfo{
	FieldName: "textField",
	FieldType: utils.IDX_TYPE_STRING_SEG,
}

var numberFieldInfo = &segment.SimpleFieldInfo{
	FieldName: "textNumber",
	FieldType: utils.IDX_TYPE_NUMBER,
}

var db *tree.BTreeDB
var mmap *utils.Mmap

func init() {
	db = tree.NewBTreeDB("./test.db")
}

func TestTextInvert_AddDocument(t *testing.T) {
	textInvert := segment.NewEmptyTextInvert(textFieldInfo.FieldType, 1, textFieldInfo.FieldName)
	_ = textInvert.AddDocument(1, "test content")
	_ = textInvert.AddDocument(2, "segment name you want to use for testing. These tests cover adding documents")

	bitMap, exists := textInvert.QueryTerm("testing")
	if exists {
		fmt.Println(bitMap.Contains(2))
	}

}

func TestTextInvert_Serialization(t *testing.T) {
	// Initialize a TextInvert instance
	textInvert := segment.NewEmptyTextInvert(textFieldInfo.FieldType, 1, textFieldInfo.FieldName)

	_ = textInvert.AddDocument(1, "test content")
	_ = textInvert.AddDocument(2, "segment name you want to use for testing.  test These tests cover adding documents")
	_ = textInvert.AddDocument(3, "segment name you want to use for testing.  test These tests cover adding documents")
	_ = textInvert.AddDocument(4, "segment name you want to use for testing.  test These tests cover adding documents")

	// Serialize the inverted index
	err := textInvert.Serialization(segmentName, db)
	if err != nil {
		return
	}

}

func TestNumberInvert_AddDocument(t *testing.T) {
	// Initialize a NumberInvert instance
	numberInvert := segment.NewEmptyNumberInvert(numberFieldInfo.FieldType, 1, numberFieldInfo.FieldName)

	// Add a document
	err := numberInvert.AddDocument(1, "1888")
	assert.NoError(t, err)

	// Verify that the document was added correctly
	bitMap, exists := numberInvert.QueryTerm(1888)
	assert.True(t, exists)
	assert.NotNil(t, bitMap)
	fmt.Println(bitMap.Contains(1))
}

func TestNumberInvert_Serialization(t *testing.T) {
	// Initialize a NumberInvert instance
	numberInvert := segment.NewEmptyNumberInvert(numberFieldInfo.FieldType, 1, numberFieldInfo.FieldName)

	//_ = numberInvert.AddDocument(1, "1888")
	//_ = numberInvert.AddDocument(3, "1888")
	//_ = numberInvert.AddDocument(5, "1888")
	_ = numberInvert.AddDocument(1, "1888")
	err := numberInvert.AddDocument(3, "123")
	fmt.Println(err)
	err = numberInvert.AddDocument(3, "1788")

	fmt.Println(err)
	_ = numberInvert.AddDocument(4, "1888")

	fmt.Println(err)
	_ = numberInvert.AddDocument(5, "1888")

	fmt.Println(err)
	_ = numberInvert.AddDocument(6, "1888")
	// Serialize the inverted index

	fmt.Println(err)
	err = numberInvert.Serialization(segmentName, db)
	fmt.Println(err)

}

func TestReadInvert(t *testing.T) {
	mmap, _ = utils.NewMmap("./s1textField_invert.idx", 0)
	text := segment.NewTextInvert(textFieldInfo.FieldType, db, textFieldInfo.FieldName, mmap)
	fmt.Println(text.QueryTerm("test"))
	mmap.Unmap()

	mmap, _ = utils.NewMmap("./s1textNumber_invert.idx", 0)
	number := segment.NewNumberInvert(numberFieldInfo.FieldType, db, numberFieldInfo.FieldName, mmap)
	fmt.Println(number.QueryTerm(123))
	fmt.Println(number.QueryRange(1788, 2333))
	mmap.Unmap()
}
