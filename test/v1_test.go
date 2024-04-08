package test

import (
	"fmt"
	"github.com/cylScripter/NexusFind/util"
	"testing"
)

func TestRandUint32(t *testing.T) {
	hashMap := util.NewConcurrentHashMap(8, 1000)
	hashMap.Set("cyl", 12)
	hashMap.Set("cyl2", 12)
	hashMap.Set("cyl3", 23)
	//hashMap.Range(func(key string, value any) error {
	//	fmt.Println(key, value)
	//	return nil
	//})
	//hashMap.Delete("cyl")
	//hashMap.Range(func(key string, value any) error {
	//	fmt.Println(key, value)
	//	return nil
	//})

	iterator := hashMap.CreateIterator()
	entry := iterator.Next()
	for entry != nil {
		fmt.Println(entry.Key, entry.Value)
		entry = iterator.Next()
	}
}
