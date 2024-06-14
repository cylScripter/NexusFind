package tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
)

type KeyInterface interface {
	ToBytes() []byte
}

type BoltHelper struct {
	name string
	db   *bolt.DB
}

func NewBoltHelper(dbname string) *BoltHelper {
	var err error
	this := &BoltHelper{name: dbname}
	this.db, err = bolt.Open(dbname, 0644, nil)
	if err != nil {
		return nil
	}
	return this
}

func (bh *BoltHelper) CreateBtree(btName string) (*bolt.Bucket, error) {
	tx, err := bh.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer func(tx *bolt.Tx) {
		_ = tx.Rollback()
	}(tx)

	table, err := tx.CreateBucketIfNotExists([]byte(btName))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return table, nil
}

func (bh *BoltHelper) Put(b *bolt.Bucket, key KeyInterface, value string) error {
	err := b.Put(key.ToBytes(), []byte(value))
	if err != nil {
		return err
	}
	return nil
}

func (bh *BoltHelper) DeleteBtree(btName string) error {
	tx, err := bh.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tx.DeleteBucket([]byte(btName))
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (bh *BoltHelper) GetBtree(btName string) (*bolt.Bucket, error) {
	tx, err := bh.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(btName))
	if b == nil {
		return nil, fmt.Errorf("tablename[%v] not found", btName)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return b, nil
}

func (bh *BoltHelper) Set(btName string, key KeyInterface, value string) error {
	err := bh.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(btName))
		if b == nil {
			return fmt.Errorf(`table-name[%v] not found`, btName)
		}
		err := bh.Put(b, key, value)
		return err
	})
	return err
}
func (bh *BoltHelper) Get(btName string, key KeyInterface) (string, error) {
	var value []byte
	err := bh.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(btName))
		value = b.Get(key.ToBytes())
		return nil
	})
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", fmt.Errorf("key[%v] Not Found", key)
	}
	return string(value), nil
}
func (bh *BoltHelper) HasKey(btName string, key KeyInterface) bool {
	if _, err := bh.Get(btName, key); err != nil {
		return false
	}
	return true

}

func (bh *BoltHelper) SetBatch(tableName string, kv map[string]string) error {
	err := bh.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return fmt.Errorf("table name[%v] not found", tableName)
		}
		for k, v := range kv {
			if err := b.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (bh *BoltHelper) GetNextKV(btName string, key int64) ([]byte, string, error) {

	var value []byte
	var bkey []byte
	bh.db.View(func(tx *bolt.Tx) error {

		var s = make([]byte, 0)
		buf := bytes.NewBuffer(s)
		binary.Write(buf, binary.BigEndian, key)

		b := tx.Bucket([]byte(btName)).Cursor()
		b.Seek(buf.Bytes())
		bkey, value = b.Next()

		return nil
	})

	if value == nil || bkey == nil {
		//bh.Logger.Error("[ERROR] Key %v not found",key)
		return nil, "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return bkey, string(value), nil

}
func (bh *BoltHelper) GetFirstKV(btName string) ([]byte, string, error) {
	var value []byte
	var key []byte
	err := bh.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(btName)).Cursor()
		key, value = b.First()
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if value == nil {

		return nil, "", fmt.Errorf("key[%v] Not Found", key)
	}
	return key, string(value), nil
}
func (bh *BoltHelper) GetRange(btName string, keyMin int64, keyMax int64) ([]string, error) {

	var s1 = make([]byte, 0)
	min := bytes.NewBuffer(s1)
	binary.Write(min, binary.BigEndian, keyMin)

	var s2 = make([]byte, 0)
	max := bytes.NewBuffer(s2)
	binary.Write(max, binary.BigEndian, keyMax)
	res := make([]string, 0)
	bh.db.View(func(tx *bolt.Tx) error {

		c := tx.Bucket([]byte(btName)).Cursor()

		for k, v := c.Seek(min.Bytes()); k != nil && bytes.Compare(k, max.Bytes()) <= 0; k, v = c.Next() {
			res = append(res, string(v))
		}

		return nil
	})

	return res, nil
}

func (bh *BoltHelper) Close() error {
	return bh.db.Close()
}
func (bh *BoltHelper) DisplayTable(tableName string) error {
	err := bh.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(tableName))
		var kv KeyInterface
		_ = b.ForEach(func(k, v []byte) error {
			buf := bytes.NewBuffer(k)
			_ = binary.Read(buf, binary.BigEndian, &kv)
			// fmt.Printf("key=%v, value=%s\n", kv, v)
			return nil
		})
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (bh *BoltHelper) BeginTx() (*bolt.Tx, error) {
	tx, err := bh.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return tx, nil
}
func (bh *BoltHelper) Commit(tx *bolt.Tx) error {
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}
