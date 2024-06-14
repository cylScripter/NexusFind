package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

const IDX_ROOT_PATH string = "./data/"

const NexusFind string = "NexusFind"

const MAX_SEGMENT_SIZE = 100000
const (
	IDX_TYPE_STRING     = 1 // 字符型索引[全词匹配]
	IDX_TYPE_STRING_SEG = 2 //字符型索引[切词匹配，全文索引,hash存储倒排]

	IDX_TYPE_NUMBER = 11 // 数字型索引，只支持整数，数字型索引只建立倒排
	IDX_TYPE_FLOAT  = 12 // 数字型索引，支持浮点数，只能保留两位小数，数字型索引只建立倒排

	IDX_TYPE_DATE = 15 // 日期型索引 '2015-11-11 00:11:12'，日期型只建立倒排，转成时间戳存储

	IDX_TYPE_PK = 21 //主键类型，倒排正排都需要，倒排使用B+树存储

	IDX_TYPE_DESC = 31 // 只存储不索引的类型

)

const STOP_WORD_FILE_PATH = "/Users/cyl/Desktop/cyl/NexusFind/utils/stopWords.txt"

// FileExist 判断文件是否存在，如果存在返回true，否则返回false
func FileExist(path string) bool {
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}

// IsDateTime function description : 判断是否是日期时间格式
// params : 字符串
// return : 是否是日期时间格式
func IsDateTime(datetime string) (int64, error) {
	var timestamp time.Time
	var err error
	if len(datetime) > 16 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	} else if len(datetime) > 10 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	} else {
		timestamp, err = time.ParseInLocation("2006-01-02", datetime, time.Local)
		if err != nil {
			return -1, err
		}
	}
	return timestamp.Unix(), nil

}

const (
	FILT_EQ    uint64 = 1 //等于
	FILT_OVER  uint64 = 2 //大于
	FILT_LESS  uint64 = 3 //小于
	FILT_RANGE uint64 = 4 //范围内
)

type SearchFilters struct {
	FieldName string  `json:"_field"`
	Start     int64   `json:"_start"`
	End       int64   `json:"_end"`
	Range     []int64 `json:"_range"`
	Type      uint64  `json:"_type"`
}

func ItoBytes(n any) []byte {
	buf := new(bytes.Buffer)
	// 将整数写入字节缓冲，使用小端序
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}
