package storage

import (
	"bytes"
	"context"
	"strings"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

type Datastore struct {
	*persistent.CarfileDB
}

func NewDatastore(db *persistent.CarfileDB) *Datastore {
	return &Datastore{CarfileDB: db}
}

func (d *Datastore) Close() error {
	return d.Close()
}

func trimPrefix(key datastore.Key) string {
	return strings.Trim(key.String(), "/")
}

// func (d *Datastore) initReplicaInfo(out *types.CarfileRecordInfo) {
// 	rs, err := d.CarfileReplicaInfosWithHash(out.CarfileHash, false)
// 	if err != nil && err != sql.ErrNoRows {
// 		return
// 	}

// 	for _, r := range rs {
// 		if r.Status == types.CacheStatusSucceeded {
// 			if r.IsCandidate {
// 				out.SucceedCandidateReplicas++
// 			} else {
// 				out.SucceedEdgeReplicas++
// 			}

// 			continue
// 		}

// 		if r.IsCandidate {
// 			out.FailedCandidateReplicas++
// 		} else {
// 			out.FailedEdgeReplicas++
// 		}
// 	}
// }

func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	out, err := d.LoadCarfileInfo(trimPrefix(key))
	if err != nil {
		return nil, err
	}
	// d.initReplicaInfo(out)
	carfile := carfileInfoFrom(out)
	valueBuf := new(bytes.Buffer)
	if err := carfile.MarshalCBOR(valueBuf); err != nil {
		return nil, err
	}

	return valueBuf.Bytes(), nil
}

func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return d.CarfileRecordExisted(trimPrefix(key))
}

func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	return d.CountCarfiles()
}

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	raw, err := d.rawQuery(ctx, q)
	if err != nil {
		return nil, err
	}

	for _, f := range q.Filters {
		raw = query.NaiveFilter(raw, f)
	}

	raw = query.NaiveOrder(raw, q.Orders...)

	// if we have filters or orders, offset and limit won't have been applied in the query
	if len(q.Filters) > 0 || len(q.Orders) > 0 {
		if q.Offset != 0 {
			raw = query.NaiveOffset(raw, q.Offset)
		}
		if q.Limit != 0 {
			raw = query.NaiveLimit(raw, q.Limit)
		}
	}

	return raw, nil
}

func (d *Datastore) rawQuery(ctx context.Context, q query.Query) (query.Results, error) {
	var rows *sqlx.Rows
	var err error

	rows, err = d.QueryCarfilesRows(ctx, q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}

	it := query.Iterator{
		Next: func() (query.Result, bool) {
			if !rows.Next() {
				return query.Result{}, false
			}

			value := &types.CarfileRecordInfo{}
			err := rows.StructScan(value)
			if err != nil {
				return query.Result{Error: err}, false
			}
			// d.initReplicaInfo(value)
			carfile := carfileInfoFrom(value)
			entry := query.Entry{Key: value.CarfileHash}
			valueBuf := new(bytes.Buffer)
			if err := carfile.MarshalCBOR(valueBuf); err != nil {
				return query.Result{Entry: entry}, false
			}

			if !q.KeysOnly {
				entry.Value = valueBuf.Bytes()
			}
			if q.ReturnsSizes {
				entry.Size = len(valueBuf.Bytes())
			}

			return query.Result{Entry: entry}, true
		},
		Close: func() error {
			return rows.Close()
		},
	}

	return query.ResultsFromIterator(q, it), nil
}

func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	carfile := &CarfileInfo{}
	if err := carfile.UnmarshalCBOR(bytes.NewReader(value)); err != nil {
		return err
	}
	return d.CreateOrUpdateCarfileRecordInfo(carfile.toCarfileRecordInfo())
}

func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	return d.RemoveCarfileRecord(trimPrefix(key))
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
