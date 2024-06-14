package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cylScripter/NexusFind/internal/index"
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/types"
	"github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
	"sync"
)

type IndexInfo struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type IndexManager struct {
	indexers       map[string]*index.Index
	indexMapLocker map[string]*sync.RWMutex
	IndexInfos     map[string]IndexInfo `json:"index_infos"`
	Logger         *utils.Log           `json:"-"`
}

func NewIndexManager(logger *utils.Log) *IndexManager {
	idm := &IndexManager{
		Logger:         logger,
		indexers:       make(map[string]*index.Index),
		indexMapLocker: make(map[string]*sync.RWMutex),
		IndexInfos:     make(map[string]IndexInfo),
	}
	// 如果之前有记录则进行反序列化
	if utils.Exist(fmt.Sprintf("%v%v.idm.meta", utils.IDX_ROOT_PATH, utils.NexusFind)) {
		metaFileName := fmt.Sprintf("%v%v.idm.meta", utils.IDX_ROOT_PATH, utils.NexusFind)
		buffer, err := utils.ReadFromJson(metaFileName)
		if err != nil {
			return idm
		}
		err = json.Unmarshal(buffer, &idm)
		if err != nil {
			return idm
		}
		for _, idxInfo := range idm.IndexInfos {
			idm.indexMapLocker[idxInfo.Name] = &sync.RWMutex{}
			idm.indexers[idxInfo.Name] = index.NewIndexFromLocalFile(idxInfo.Name, idxInfo.Path, logger)
		}
	}
	return idm
}

func (idm *IndexManager) GetIndex(indexName string) *index.Index {
	if idm.indexMapLocker[indexName] == nil {
		return nil
	}
	idm.indexMapLocker[indexName].RLock()
	defer idm.indexMapLocker[indexName].RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		idm.Logger.NFLog.Warningf("Index [%v] does not exist", indexName)
		return nil
	}
	idx := idm.indexers[indexName]
	return idx
}

func (idm *IndexManager) CreateIndex(indexName string, fields []segment.SimpleFieldInfo) error {
	idm.indexMapLocker[indexName] = &sync.RWMutex{}
	idm.indexMapLocker[indexName].Lock()
	defer idm.indexMapLocker[indexName].Unlock()
	if _, ok := idm.indexers[indexName]; ok {
		idm.Logger.NFLog.Warningf("Index [%v] already exists", indexName)
		return nil
	}
	idm.indexers[indexName] = index.NewEmptyIndex(indexName, utils.IDX_ROOT_PATH, idm.Logger)
	idm.IndexInfos[indexName] = IndexInfo{Name: indexName, Path: utils.IDX_ROOT_PATH}
	idm.indexers[indexName].SetFields(fields)
	return idm.storeIndexManager()
}

func (idm *IndexManager) storeIndexManager() error {
	metaFileName := fmt.Sprintf("%v%v.idm.meta", utils.IDX_ROOT_PATH, utils.NexusFind)
	if err := utils.WriteToJson(idm, metaFileName); err != nil {

		return err
	}
	return nil
}

func (idm *IndexManager) Add(indexName string, doc *doc.Document) (uint64, error) {
	if _, ok := idm.indexers[indexName]; !ok {
		return 0, fmt.Errorf("no has %v", indexName)
	}
	idm.indexMapLocker[indexName].Lock()
	defer idm.indexMapLocker[indexName].Unlock()
	return idm.indexers[indexName].AddDocument(doc)
}

func (idm *IndexManager) Get(indexName string, id string) (*doc.Document, bool) {
	idm.indexMapLocker[indexName].RLock()
	defer idm.indexMapLocker[indexName].RUnlock()
	docpk, exits := idm.indexers[indexName].IsNotDelete(id)
	if exits {
		return idm.indexers[indexName].GetDocument(docpk)
	}
	return nil, false
}

func (idm *IndexManager) Delete(indexName string, pk string) error {
	idm.indexMapLocker[indexName].RLock()
	defer idm.indexMapLocker[indexName].RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		return fmt.Errorf("index[%v] not found", indexName)
	}
	return idm.indexers[indexName].DeleteDocument(pk)
}

func (idm *IndexManager) Search(indexName string, query *types.TermQuery, filters []*types.SearchFilters) []*doc.Document {
	idm.indexMapLocker[indexName].RLock()
	defer idm.indexMapLocker[indexName].RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		fmt.Println("")
		return nil
	}
	return idm.indexers[indexName].Search(query, filters)
}

func (idm *IndexManager) sync(indexName string) error {
	idm.indexMapLocker[indexName].RLock()
	defer idm.indexMapLocker[indexName].RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		return errors.New(fmt.Sprintf("[ERROR] index[%v] not found", indexName))
	}
	return idm.indexers[indexName].SyncMemorySegment()
}

func (idm *IndexManager) Close() error {
	for _, idxInfo := range idm.IndexInfos {
		err := idm.sync(idxInfo.Name)
		if err != nil {
			fmt.Println(err)
		}
		err = idm.indexers[idxInfo.Name].Close()
		if err != nil {
			fmt.Println(err)
		}
	}
	return idm.storeIndexManager()
}
