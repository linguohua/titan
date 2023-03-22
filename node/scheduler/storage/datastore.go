package storage

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

type Datastore struct {
	sync.RWMutex
	db    *db.SqlDB
	local *datastore.MapDatastore
	dtypes.ServerID
}

func NewDatastore(db *db.SqlDB, serverID dtypes.ServerID) *Datastore {
	return &Datastore{
		db:       db,
		local:    datastore.NewMapDatastore(),
		ServerID: serverID,
	}
}

func (d *Datastore) Close() error {
	return d.local.Close()
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.Get(ctx, key)
}

func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.Has(ctx, key)
}

func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	d.RLock()
	defer d.RUnlock()
	return d.local.GetSize(ctx, key)
}

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	rows, err = d.db.QueryUnfinishedCarfiles(ctx, q.Limit, q.Offset, d.ServerID)
	if err != nil {
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
			continue
		}

		in.ReplicaInfos, err = d.db.GetReplicaInfos(in.CarfileHash, true)
		if err != nil {
			continue
		}

		carfile := carfileInfoFrom(in)
		valueBuf := new(bytes.Buffer)
		if err = carfile.MarshalCBOR(valueBuf); err != nil {
			log.Errorf("carfile marshal cbor: %v", err)
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

func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	d.Lock()
	defer d.Unlock()

	if err := d.local.Put(ctx, key, value); err != nil {
		log.Errorf("datastore local put: %v", err)
	}
	carfile := &CarfileInfo{}
	if err := carfile.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}
	if carfile.CarfileHash == "" {
		return nil
	}

	info := carfile.ToCarfileRecordInfo()
	info.ServerID = d.ServerID

	return d.db.UpsertCarfileRecord(info)
}

func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	d.Lock()
	defer d.Unlock()

	if err := d.local.Delete(ctx, key); err != nil {
		log.Errorf("datastore local delete: %v", err)
	}
	return d.db.RemoveCarfileRecord(trimPrefix(key))
}

func (d *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return &batch{
		ds:  d,
		ops: make(map[datastore.Key]op),
	}, nil
}

var _ datastore.Batching = (*Datastore)(nil)
