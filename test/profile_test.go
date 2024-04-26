package test

import (
	"fmt"
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/internal/index/tree"
	"github.com/cylScripter/NexusFind/utils"
	"testing"
)

var profileDb *tree.BTreeDB
var profileMmap *utils.Mmap

func init() {

	profileDb = tree.NewBTreeDB("./profile.db")
	profileMmap, _ = utils.NewMmap("./profile.idx", 0)
}

func TestProfile(t *testing.T) {
	profile := segment.NewProfile(segmentName, 1, 100, profileMmap, profileDb)
	//for i := 1; i < 100; i++ {
	//	profile.AddDocument(uint64(i), &doc.Document{
	//		Id:       "34343",
	//		Keywords: []*doc.KeyWord{{Word: "test", WordTF: 23}},
	//		Content: map[string]string{
	//			"title": "Document Two Title", "body": "This is the content of document two.",
	//		},
	//	})
	//}
	////
	//
	fmt.Println(profile.Query(100))
	//profile.SyncProfile()

}
