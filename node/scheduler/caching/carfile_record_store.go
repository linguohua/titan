package caching

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

// CarfileRecordStore carfile record datastore
type CarfileRecordStore struct {
	sync.RWMutex
	sqlDB *db.SQLDB
	local *datastore.MapDatastore
	dtypes.ServerID
}

// NewDatastore new
func NewDatastore(db *db.SQLDB, serverID dtypes.ServerID) *CarfileRecordStore {
	return &CarfileRecordStore{
		sqlDB:    db,
		local:    datastore.NewMapDatastore(),
		ServerID: serverID,
	}
}

// Close close
func (d *CarfileRecordStore) Close() error {
	return d.local.Close()
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// Get get data
func (d *CarfileRecordStore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.Get(ctx, key)
}

// Has has key
func (d *CarfileRecordStore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.Has(ctx, key)
}

// GetSize get data size
func (d *CarfileRecordStore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.GetSize(ctx, key)
}

// Query load carfile record infos
func (d *CarfileRecordStore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	state := append(FailedStates, CachingStates...)

	rows, err = d.sqlDB.LoadCarfileRecords(state, q.Limit, q.Offset, d.ServerID)
	if err != nil {
		log.Errorf("LoadCarfileRecords :%s", err.Error())
		return nil, err
	}

	d.Lock()
	defer d.Unlock()

	re := make([]query.Entry, 0)
	// loading carfiles to local
	for rows.Next() {
		in := &types.CarfileRecordInfo{}
		err = rows.StructScan(in)
		if err != nil {
			log.Errorf("carfile StructScan err: %s", err.Error())
			continue
		}

		in.ReplicaInfos, err = d.sqlDB.LoadReplicaInfosOfCarfile(in.CarfileHash, true)
		if err != nil {
			log.Errorf("carfile %s load replicas err: %s", in.CarfileCID, err.Error())
			continue
		}

		carfile := carfileCacheInfoFrom(in)
		valueBuf := new(bytes.Buffer)
		if err = carfile.MarshalCBOR(valueBuf); err != nil {
			log.Errorf("carfile marshal cbor: %s", err.Error())
			continue
		}

		prefix := "/"
		entry := query.Entry{
			Key: prefix + carfile.CarfileHash.String(), Size: len(valueBuf.Bytes()),
		}

		if err = d.local.Put(ctx, datastore.NewKey(entry.Key), valueBuf.Bytes()); err != nil {
			log.Errorf("datastore loading carfiles: %v", err)
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

// Put update carfile record info
func (d *CarfileRecordStore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	d.Lock()
	defer d.Unlock()

	if err := d.local.Put(ctx, key, value); err != nil {
		log.Errorf("datastore local put: %v", err)
	}
	carfile := &CarfileCacheInfo{}
	if err := carfile.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}
	if carfile.CarfileHash == "" {
		return nil
	}

	info := carfile.ToCarfileRecordInfo()
	info.ServerID = d.ServerID

	return d.sqlDB.UpsertCarfileRecord(info)
}

// Delete delete carfile record info
func (d *CarfileRecordStore) Delete(ctx context.Context, key datastore.Key) error {
	d.Lock()
	defer d.Unlock()

	if err := d.local.Delete(ctx, key); err != nil {
		log.Errorf("datastore local delete: %v", err)
	}
	return d.sqlDB.RemoveCarfileRecord(trimPrefix(key))
}

// Sync sync
func (d *CarfileRecordStore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Batch batch
func (d *CarfileRecordStore) Batch(ctx context.Context) (datastore.Batch, error) {
	return d.local.Batch(ctx)
}

var _ datastore.Batching = (*CarfileRecordStore)(nil)
