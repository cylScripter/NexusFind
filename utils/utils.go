package utils

import (
	"encoding/json"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"io"
	"os"
)

// ReadFromJson function description : 读取json文件
// params :
// return :
func ReadFromJson(file_name string) ([]byte, error) {
	fin, err := os.Open(file_name)
	defer fin.Close()
	if err != nil {
		return nil, err
	}
	buffer, err := io.ReadAll(fin)
	if err != nil {
		return nil, err
	}
	return buffer, nil

}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

// WriteToJson function description : 写入json文件
// params :
// return :
func WriteToJson(data interface{}, file_name string) error {

	//fmt.Printf("Writing to File [%v]...\n", file_name)
	info_json, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Marshal err %v\n", file_name)
		return err
	}

	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		return err
	}
	fout.Write(info_json)
	return nil

}

func ReadBitMap(path string) (*roaring64.Bitmap, error) {
	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fi.Close()
	fd, err := io.ReadAll(fi)
	idxBitMap := roaring64.New()
	err = idxBitMap.UnmarshalBinary(fd)
	if err != nil {
		return nil, err
	}
	return idxBitMap, nil
}

func WriteBitMap(path string, bitmap *roaring64.Bitmap) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = bitmap.WriteTo(file)
	if err != nil {
		return err
	}
	return nil
}
