package test

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"testing"
	"time"
)

func generateRandomValue(length int) (string, error) {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes)[:length], nil
}

type T string

func (tt T) ToBytes() []byte {
	return []byte(tt)
}

func TestBtree(t *testing.T) {
	var bd *tree.BTreeDB
	bd = tree.NewBTreeDB("./test2.bt")
	te := "tete"
	idList := make([]string, 0)
	for i := 0; i < 1000; i++ {
		fmt.Println(i)
		id, _ := generateRandomValue(10)
		if i < 10 {
			fmt.Println(id)
			idList = append(idList, id)
		}
		start := time.Now() // 记录开始时间
		bd.Set(te, T(id), 1)
		elapsed := time.Since(start) // 计算经过的时间
		fmt.Printf("代码运行时间：%v:%v\n", i, elapsed)
	}
	err := db.Close()
	if err != nil {
		return
	}
}

func TestReadData(t *testing.T) {
	var bd *tree.BTreeDB
	bd = tree.NewBTreeDB("./test2.bt")
	te := "tete"

	start := time.Now() // 记录开始时间
	fmt.Println(bd.Search(te, T("y_7ZJrEMkX")))

	elapsed := time.Since(start) // 计算经过的时间
	fmt.Printf("代码运行时间：%v\n", elapsed)

	err := db.Close()
	if err != nil {
		return
	}
}
