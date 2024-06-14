package test

import (
	"encoding/csv"
	"fmt"
	doc2 "github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestMn(t *testing.T) {
	result := make([]doc2.Document, 0)
	// 打开CSV文件
	file, err := os.Open("bili_video.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// 创建一个CSV读取器
	reader := csv.NewReader(file)

	// 逐行读取
	for {
		data := make(map[string]string)
		var document doc2.Document
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
		}
		id := strings.Split(record[0], "/")
		//segmented := utils.GetGseSegmenter()
		//segResult := segmented.CutSearch(record[1], false)
		inputLayout := "2006/1/2 15:04"

		ts, err := time.Parse(inputLayout, record[2])
		outputLayout := "2006-01-02 15:04"

		data["id"] = id[len(id)-1] + randString(3)
		data["content"] = record[1]
		data["times"] = ts.Format(outputLayout)
		data["likeCount"] = record[4]
		data["author"] = record[3]
		data["url"] = record[0]
		data["category"] = record[9]
		document = doc2.Document{
			Id:       data["id"],
			Keywords: make([]*doc2.KeyWord, 0),
			Content:  data,
		}
		segg := utils.GetGseSegmenter()
		keys := segg.CutSearch(record[9], false)
		fmt.Println(keys)
		fmt.Println(doc2.TF(keys))
		result = append(result, document)

	}
}

func ReadData() []doc2.Document {
	result := make([]doc2.Document, 0)
	segg := utils.GetGseSegmenter()
	// 打开CSV文件
	file, err := os.Open("bili_video.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// 创建一个CSV读取器
	reader := csv.NewReader(file)

	// 逐行读取
	for {
		data := make(map[string]string)
		var document doc2.Document
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
		}
		id := strings.Split(record[0], "/")
		//segmented := utils.GetGseSegmenter()
		//segResult := segmented.CutSearch(record[1], false)
		inputLayout := "2006/1/2 15:04"

		ts, err := time.Parse(inputLayout, record[2])
		outputLayout := "2006-01-02 15:04"

		keys := segg.CutSearch(record[9], false)
		data["id"] = id[len(id)-1] + randString(3)
		data["content"] = record[1]
		data["times"] = ts.Format(outputLayout)
		data["likeCount"] = record[4]
		data["author"] = record[3]
		data["url"] = record[0]
		data["category"] = record[9]
		document = doc2.Document{
			Id:       data["id"],
			Keywords: doc2.TF(keys),
			Content:  data,
		}
		result = append(result, document)

	}
	return result
}
