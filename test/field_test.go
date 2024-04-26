package test

import (
	"fmt"
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"testing"
)

func TestTextFiledAddDocument(t *testing.T) {
	testField := segment.NewEmptyField(textFieldInfo.FieldName, 1, textFieldInfo.FieldType)
	err := testField.AddDocument(1, "test content")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(testField.Query("test"))
}
