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
	GetTopChecksum(ctx context.Context) (string, error)
	GetBucketChecksums(ctx context.Context) (map[uint32]string, error)
	GetCarsOfBucketLocal(ctx context.Context, bucketID uint32) ([]cid.Cid, error)
	GetCarsOfBucketRemote(ctx context.Context, bucketID uint32) ([]cid.Cid, error)
	DeleteCar(root cid.Cid) error
	AddLostCar(root cid.Cid) error
}

func NewDataSync(sync Sync) *DataSync {
	return &DataSync{sync}
}

// CompareTopChecksums can check asset if same as scheduler
// topChecksum is checksum of all buckets
func (ds *DataSync) CompareTopChecksum(ctx context.Context, topChecksum string) (bool, error) {
	checksum, err := ds.GetTopChecksum(ctx)
	if err != nil {
		return false, err
	}

	return checksum == topChecksum, nil
}

// CompareBucketChecksums group asset in bucket, and compare single bucket checksum
//  checksums are list of bucket checksum
func (ds *DataSync) CompareBucketChecksums(ctx context.Context, checksums map[uint32]string) ([]uint32, error) {
	localChecksums, err := ds.GetBucketChecksums(ctx)
	if err != nil {
		return nil, err
	}

	mismatchBuckets := make([]uint32, 0)
	lostBuckets := make([]uint32, 0)

	for k, checksum := range checksums {
		if cs, ok := localChecksums[k]; ok {
			if cs != checksum {
				mismatchBuckets = append(mismatchBuckets, k)
			}
			delete(localChecksums, k)
		} else {
			lostBuckets = append(lostBuckets, k)
		}
	}

	extraBuckets := make([]uint32, 0)
	for k := range localChecksums {
		extraBuckets = append(extraBuckets, k)
	}

	go ds.doSync(ctx, extraBuckets, lostBuckets, mismatchBuckets)

	return append(mismatchBuckets, lostBuckets...), nil
}

func (ds *DataSync) doSync(ctx context.Context, extraBuckets, lostBuckets, mismatchBuckets []uint32) {
	if len(extraBuckets) > 0 {
		ds.removeExtraAsset(ctx, extraBuckets)
	}

	if len(lostBuckets) > 0 {
		ds.addLostAsset(ctx, lostBuckets)
	}

	if len(mismatchBuckets) > 0 {
		ds.repairMismatchAsset(ctx, mismatchBuckets)
	}
}

func (ds *DataSync) removeExtraAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetCarsOfBucketLocal(ctx, bucketID)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.DeleteCar(car)
	}
	return nil
}

func (ds *DataSync) addLostAsset(ctx context.Context, buckets []uint32) error {
	cars := make([]cid.Cid, 0)
	for _, bucketID := range buckets {
		cs, err := ds.GetCarsOfBucketLocal(ctx, bucketID)
		if err != nil {
			return err
		}
		cars = append(cars, cs...)
	}

	for _, car := range cars {
		ds.AddLostCar(car)
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
		ds.DeleteCar(car)
	}

	for _, car := range lostCars {
		ds.AddLostCar(car)
	}
	return nil
}

// return extra cars and lost cars
func (ds *DataSync) compareBuckets(ctx context.Context, bucketID uint32) ([]cid.Cid, []cid.Cid, error) {
	localCars, err := ds.GetCarsOfBucketLocal(ctx, bucketID)
	if err != nil {
		return nil, nil, err
	}
	remoteCars, err := ds.GetCarsOfBucketRemote(ctx, bucketID)
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
