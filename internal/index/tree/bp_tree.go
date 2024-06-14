package tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"strconv"
)

type BTreeDB struct {
	filename string
	dbHelper *BoltHelper
	buckets  map[string]*bolt.Tx
}

func NewBTreeDB(dbname string) *BTreeDB {
	this := &BTreeDB{filename: dbname, dbHelper: nil, buckets: make(map[string]*bolt.Tx)}
	this.dbHelper = NewBoltHelper(dbname)
	return this
}

func (db *BTreeDB) AddBTree(name string) error {
	_, err := db.dbHelper.CreateBtree(name)
	return err
}

func (db *BTreeDB) Set(btName string, key KeyInterface, value uint64) error {
	return db.dbHelper.Set(btName, key, fmt.Sprintf("%v", value))
}

func (db *BTreeDB) SetBatch(btName string, kv map[string]string) error {
	return db.dbHelper.SetBatch(btName, kv)
}

func (db *BTreeDB) Search(btName string, key KeyInterface) (bool, uint64) {
	visitor, err := db.dbHelper.Get(btName, key)
	if err != nil {
		return false, 0
	}
	//db.logger.Info("Search btName : %v  key : %v value str : %v ",btName,key,visitor)
	res, e := strconv.ParseUint(visitor, 10, 64)
	if e != nil {
		return false, 0
	}
	//db.logger.Info("Search btName : %v  key : %v value  : %v ",btName,key,u)
	return true, res
}

func (db *BTreeDB) HasKey(btName string, key KeyInterface) bool {
	return db.dbHelper.HasKey(btName, key)
}
func (db *BTreeDB) Put(b *bolt.Bucket, key KeyInterface, value uint64) error {
	return db.dbHelper.Put(b, key, fmt.Sprintf("%v", value))
}
func (db *BTreeDB) SearchRange(btName string, keyMin, keyMax int64) (bool, []uint64) {
	if keyMin > keyMax {
		return false, nil
	}
	// fmt.Println(db == nil)
	vstr, err := db.dbHelper.GetRange(btName, keyMin, keyMax)
	if err != nil {
		return false, nil
	}
	res := make([]uint64, 0, 10)
	for _, v := range vstr {
		u, e := strconv.ParseUint(v, 10, 64)
		if e != nil {
			return false, nil
		}
		res = append(res, u)
	}
	if len(res) == 0 {
		return false, res
	}
	return true, res
}

func (db *BTreeDB) GetFirstKV(btName string) (int64, uint64, bool) {
	key, vstr, err := db.dbHelper.GetFirstKV(btName)
	if err != nil {
		fmt.Printf("err: %v", err)
		return -1, 0, false
	}
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		fmt.Printf("err: %v", err)
		return -1, 0, false
	}
	buf := bytes.NewBuffer(key)
	var kv int64
	binary.Read(buf, binary.BigEndian, &kv)
	return kv, u, true
}

func (db *BTreeDB) GetNextKV(btname string, key int64) (int64, uint64, bool) {
	vkey, vstr, err := db.dbHelper.GetNextKV(btname, key)
	if err != nil {
		return -1, 0, false
	}
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return -1, 0, false
	}

	buf := bytes.NewBuffer(vkey)
	var kv int64
	binary.Read(buf, binary.BigEndian, &kv)
	return kv, u, true
}

func (db *BTreeDB) Close() error {
	return db.dbHelper.Close()
}

func (db *BTreeDB) BeginTx() (tx *bolt.Tx, err error) {
	return db.dbHelper.BeginTx()
}

func (db *BTreeDB) Commit(tx *bolt.Tx) error {
	return db.dbHelper.Commit(tx)
}
