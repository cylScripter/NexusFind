package test

import (
	"context"
	"fmt"
	"github.com/cylScripter/NexusFind/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup

func TestConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond) //控制连接超时
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		"localhost:50001",
		grpc.WithTransportCredentials(insecure.NewCredentials()), //Credential即使为空，也必须设置
		//grpc.Dial是异步连接的，连接状态为正在连接。但如果你设置了 grpc.WithBlock 选项，就会阻塞等待（等待握手成功）。另外你需要注意，当未设置 grpc.WithBlock 时，ctx 超时控制对其无任何效果。
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Println(conn)
	}
	var fields []*engine.SimpleFieldInfo
	fields = make([]*engine.SimpleFieldInfo, 0)
	for _, iter := range FieldInfo {
		item := &engine.SimpleFieldInfo{FieldName: iter.FieldName, FieldType: iter.FieldType}
		fields = append(fields, item)
	}
	client := engine.NewIndexServiceClient(conn)
	code, err := client.CreateIndex(context.Background(), &engine.CreateIndexRequest{IndexName: "test", FieldInfo: fields})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(code)
	//doc := &ReadData()[0]
	//fmt.Println(doc)
	//add, err := client.Add(context.Background(), &engine.AddRequest{IndexName: "test", Doc: doc})
	//if err != nil {
	//	return
	//}
	//fmt.Println(code, add)
	//for i := 1; i < 333; i++ {
	//	data := ReadData()
	//	for _, item := range data {
	//		wg.Add(1) // 启动一个goroutine就登记+1
	//		go func() {
	//			fmt.Println(item)
	//			client.Add(context.Background(), &engine.AddRequest{IndexName: "test", Doc: &item})
	//			defer wg.Done()
	//		}()
	//	}
	//	wg.Wait() // 等待所有登记的goroutine都结束
	//}

	//start := time.Now()
	//que := types.NewTermQuery("content", "框架")
	//search, err := client.Search(context.Background(), &engine.SearchRequest{
	//	IndexName: "test",
	//	Query:     que,
	//	Filter:    nil,
	//})
	//
	//duration := time.Now().Sub(start)
	//fmt.Printf("代码运行时间: %v\n", duration)
	//
	//fmt.Println(search)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	get, err := client.Get(context.Background(), &engine.DocIdRequest{IndexName: "test", DocId: "BV1xt4y1R7A8Jds"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(get)
}

//BV1xt4y1R7A8Jds
