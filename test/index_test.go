package test

import (
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/utils"
	"testing"
)

// import (
//
//	"fmt"
//	"github.com/cylScripter/NexusFind/engine"
//	"github.com/cylScripter/NexusFind/internal/index"
//	"github.com/cylScripter/NexusFind/types/doc"
//
//	"github.com/cylScripter/NexusFind/internal/index/segment"
//	"github.com/cylScripter/NexusFind/types"
//	"github.com/cylScripter/NexusFind/utils"
//	"testing"
//	"time"
//
// )
var path string = "./data/"
var indexName string = "test"

var FieldInfo []segment.SimpleFieldInfo = []segment.SimpleFieldInfo{
	{"id", utils.IDX_TYPE_PK},
	{"times", utils.IDX_TYPE_DATE},
	{"likeCount", utils.IDX_TYPE_NUMBER},
	{"content", utils.IDX_TYPE_STRING_SEG},
	{"category", utils.IDX_TYPE_STRING_SEG},
	{"url", utils.IDX_TYPE_DESC},
	{"author", utils.IDX_TYPE_STRING},
}

func TestEmptyIndex(t *testing.T) {
	//emptyIndex := index.NewEmptyIndex(indexName, path)
	//emptyIndex.SetFields(FieldInfo)
	//for i := 0; i < 334; i++ {
	//	data := ReadData()
	//	for _, d := range data {
	//		fmt.Println(emptyIndex.AddDocument(&d))
	//	}
	//}
	//fmt.Println(emptyIndex.SyncMemorySegment())
	//fmt.Println(emptyIndex.GetDocument(4))
}

func TestLoadIndex(t *testing.T) {
	//que := types.NewTermQuery("content", "框架")
	//queA := types.NewTermQuery("author", "郭宏志-老郭")
	//queB := types.NewTermQuery("category", "毕业")
	//
	//Index := index.NewIndexFromLocalFile(indexName, path)
	////fmt.Println(Index.GetDocument(33))
	//start := time.Now()
	//fmt.Println(que.Keyword)
	////fmt.Println()
	////Index.Search(que.And(queA), nil)
	//result := make([]*doc.Document, 0)
	//for _, iter := range Index.Search(que.And(queA).And(queB), nil) {
	//	result = append(result, iter)
	//}
	//duration := time.Now().Sub(start)
	//fmt.Printf("代码运行时间: %v\n", duration)
	//fmt.Println(len(result))
	//////
	//fmt.Println(Index.GetDocument(100073))
	////data := ReadData()
	////for _, d := range data {
	////	Index.AddDocument(&d)
	////}
	////fmt.Println(que.Keyword)
	//fmt.Println(Index.SyncMemorySegment())
	//
	//fmt.Println(len(Index.Search(que, nil)))

}

func TestEngine(t *testing.T) {
	//idm := engine.NewIndexManager()
	//idm.GetIndex(indexName)
	//
	////Index := idm.GetIndex(indexName)
	//que := types.NewTermQuery("content", "框架")
	//fmt.Println(len(idm.Search(indexName, que, nil)))
	////fmt.Println(Index.GetDocument(33))
	//
	//fmt.Println(idm.Get(indexName, "BV1xM411i7XCpaL"))
	//
	//err := idm.Close()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//err := idm.CreateIndex(indexName, FieldInfo)
	//if err != nil {
	//	fmt.Println(err)
	//}

}
