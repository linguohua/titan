package sync

import (
	"context"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("datasync")

type DataSync struct {
	Sync
}

type Sync interface {
	// GetTopChecksum local top checksum
	GetTopHash(ctx context.Context) (string, error)
	// GetBucketChecksums local checksums of all buckets
	GetBucketHashes(ctx context.Context) (map[uint32]string, error)
	// GetAssetsOfBucket isLocalOrRemote default false is local, true is remote
	GetAssetsOfBucket(ctx context.Context, bucketNumber uint32, isRemote bool) ([]cid.Cid, error)
	DeleteAsset(root cid.Cid) error
	AddLostAsset(root cid.Cid) error
}

func NewDataSync(sync Sync) *DataSync {
	return &DataSync{sync}
}

// CompareTopChecksums can check asset if same as scheduler
// topChecksum is checksum of all buckets
func (ds *DataSync) CompareTopHash(ctx context.Context, topHash string) (bool, error) {
	hash, err := ds.GetTopHash(ctx)
	if err != nil {
		return false, err
	}

	return hash == topHash, nil
}

// CompareBucketChecksums group asset in bucket, and compare single bucket checksum
//  checksums are list of bucket checksum
func (ds *DataSync) CompareBucketHashes(ctx context.Context, hashes map[uint32]string) ([]uint32, error) {
	localHashes, err := ds.GetBucketHashes(ctx)
	if err != nil {
		return nil, err
	}

	mismatchBuckets := make([]uint32, 0)
	lostBuckets := make([]uint32, 0)

	for k, hash := range hashes {
		if h, ok := localHashes[k]; ok {
			if h != hash {
				mismatchBuckets = append(mismatchBuckets, k)
			}
			delete(localHashes, k)
		} else {
			lostBuckets = append(lostBuckets, k)
		}
	}

	extraBuckets := make([]uint32, 0)
	for k := range localHashes {
		extraBuckets = append(extraBuckets, k)
	}

	go ds.doSync(ctx, extraBuckets, lostBuckets, mismatchBuckets)

	return append(mismatchBuckets, lostBuckets...), nil
}

func (ds *DataSync) doSync(ctx context.Context, extraBuckets, lostBuckets, mismatchBuckets []uint32) {
	if len(extraBuckets) > 0 {
		if err := ds.removeExtraAsset(ctx, extraBuckets); err != nil {
			log.Errorf("remove extra asset error:%s", err.Error())
		}
	}

	if len(lostBuckets) > 0 {
		if err := ds.addLostAsset(ctx, lostBuckets); err != nil {
			log.Errorf("add lost asset error:%s", err.Error())
		}
	}

	if len(mismatchBuckets) > 0 {
		if err := ds.repairMismatchAsset(ctx, mismatchBuckets); err != nil {
			log.Errorf("repair mismatch asset error %s", err.Error())
		}
	}
}

func (ds *DataSync) removeExtraAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.DeleteAsset(car)
	}
	return nil
}

func (ds *DataSync) addLostAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.AddLostAsset(car)
	}
	return nil
}

func (ds *DataSync) repairMismatchAsset(ctx context.Context, buckets []uint32) error {
	extraCars := make([]cid.Cid, 0)
	lostCars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		extras, lost, err := ds.compareBuckets(ctx, bucketID)
		if err != nil {
			return err
		}

		if len(extras) > 0 {
			extraCars = append(extraCars, extras...)
		}

		if len(lost) > 0 {
			lostCars = append(lostCars, lost...)
		}

	}

	for _, car := range extraCars {
		ds.DeleteAsset(car)
	}

	for _, car := range lostCars {
		ds.AddLostAsset(car)
	}
	return nil
}

// return extra cars and lost cars
func (ds *DataSync) compareBuckets(ctx context.Context, bucketID uint32) ([]cid.Cid, []cid.Cid, error) {
	localCars, err := ds.GetAssetsOfBucket(ctx, bucketID, false)
	if err != nil {
		return nil, nil, err
	}
	remoteCars, err := ds.GetAssetsOfBucket(ctx, bucketID, true)
	if err != nil {
		return nil, nil, err
	}

	return ds.compareCars(ctx, localCars, remoteCars)
}

// return extra cars and lost cars
func (ds *DataSync) compareCars(ctx context.Context, localCars []cid.Cid, remoteCars []cid.Cid) ([]cid.Cid, []cid.Cid, error) {
	localCarMap := make(map[string]cid.Cid, 0)
	for _, car := range localCars {
		localCarMap[car.Hash().String()] = car
	}

	lostCars := make([]cid.Cid, 0)
	for _, car := range remoteCars {
		if _, ok := localCarMap[car.Hash().String()]; ok {
			delete(localCarMap, car.Hash().String())
		} else {
			lostCars = append(lostCars, car)
		}
	}

	extraCars := make([]cid.Cid, 0)
	for _, car := range localCarMap {
		extraCars = append(extraCars, car)
	}
	return extraCars, lostCars, nil
}
