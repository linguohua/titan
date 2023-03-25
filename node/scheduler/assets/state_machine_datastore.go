package assets

import (
	"bytes"
	"context"
	"strings"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/scheduler/db"
)

// DataStore  asset datastore
type DataStore struct {
	sync.RWMutex
	assetDB *db.SQLDB
	assetDS *datastore.MapDatastore
	dtypes.ServerID
}

// NewDatastore new
func NewDatastore(db *db.SQLDB, serverID dtypes.ServerID) *DataStore {
	return &DataStore{
		assetDB:  db,
		assetDS:  datastore.NewMapDatastore(),
		ServerID: serverID,
	}
}

// Close close
func (d *DataStore) Close() error {
	return d.assetDS.Close()
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// Get get data
func (d *DataStore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.Get(ctx, key)
}

// Has has key
func (d *DataStore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.Has(ctx, key)
}

// GetSize get data size
func (d *DataStore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.assetDS.GetSize(ctx, key)
}

// Query load asset infos
func (d *DataStore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	state := append(FailedStates, CachingStates...)

	rows, err = d.assetDB.LoadAssetRecords(state, q.Limit, q.Offset, d.ServerID)
	if err != nil {
		log.Errorf("LoadAssets :%s", err.Error())
		return nil, err
	}

	d.Lock()
	defer d.Unlock()

	re := make([]query.Entry, 0)
	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = d.assetDB.LoadAssetReplicaInfos(cInfo.Hash)
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		asset := assetCachingInfoFrom(cInfo)
		valueBuf := new(bytes.Buffer)
		if err = asset.MarshalCBOR(valueBuf); err != nil {
			log.Errorf("asset marshal cbor: %s", err.Error())
			continue
		}

		prefix := "/"
		entry := query.Entry{
			Key: prefix + asset.Hash.String(), Size: len(valueBuf.Bytes()),
		}

		if err = d.assetDS.Put(ctx, datastore.NewKey(entry.Key), valueBuf.Bytes()); err != nil {
			log.Errorf("datastore loading assets: %v", err)
		}

		if !q.KeysOnly {
			entry.Value = valueBuf.Bytes()
		}

		re = append(re, entry)
	}

	r := query.ResultsWithEntries(q, re)
	r = query.NaiveQueryApply(q, r)

	return r, nil
}

// Put update asset record info
func (d *DataStore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	d.Lock()
	defer d.Unlock()

	if err := d.assetDS.Put(ctx, key, value); err != nil {
		log.Errorf("datastore local put: %v", err)
	}
	aInfo := &AssetCachingInfo{}
	if err := aInfo.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}
	if aInfo.Hash == "" {
		return nil
	}

	info := aInfo.ToAssetRecord()
	info.ServerID = d.ServerID

	return d.assetDB.UpsertAssetRecord(info)
}

// Delete delete asset record info
func (d *DataStore) Delete(ctx context.Context, key datastore.Key) error {
	d.Lock()
	defer d.Unlock()

	if err := d.assetDS.Delete(ctx, key); err != nil {
		log.Errorf("datastore local delete: %v", err)
	}
	return d.assetDB.RemoveAssetRecord(trimPrefix(key))
}

// Sync sync
func (d *DataStore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Batch batch
func (d *DataStore) Batch(ctx context.Context) (datastore.Batch, error) {
	return d.assetDS.Batch(ctx)
}

var _ datastore.Batching = (*DataStore)(nil)
