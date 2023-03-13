package store

import (
	"fmt"
	"os"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"
)

type dagstoreOpts struct {
	carsDir       string
	indexRepo     string
	topIndexDir   string
	transientsDir string
}

// dagstore can not delete data from FullIndexRepo,topLevelIndex
// so export FullIndexRepo, topLevelIndex that can delete data from it
type dagstoreWrapper struct {
	dagst     *dagstore.DAGStore
	indexRepo index.FullIndexRepo
	topIndex  index.Inverted
}

func newDatastore(dir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, xerrors.Errorf("failed to create directory %s for DAG store datastore: %w", dir, err)
	}

	// Create a new LevelDB datastore
	return levelds.NewDatastore(dir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
}

func newDagstore(opts *dagstoreOpts) (*dagstoreWrapper, error) {
	err := os.MkdirAll(opts.transientsDir, 0o755)
	if err != nil {
		return nil, err
	}

	reg := mount.NewRegistry()
	err = reg.Register("fs", &mount.FSMount{FS: os.DirFS(opts.carsDir)})
	if err != nil {
		fmt.Printf("Register error:%s", err.Error())
		return nil, err
	}

	irepo, err := index.NewFSRepo(opts.indexRepo)
	if err != nil {
		return nil, err
	}

	ds, err := newDatastore(opts.topIndexDir)
	if err != nil {
		return nil, err
	}

	topIndex := index.NewInverted(ds)

	dagst, err := dagstore.NewDAGStore(dagstore.Config{
		MountRegistry: reg,
		TransientsDir: opts.transientsDir,
		Datastore:     ds,
		IndexRepo:     irepo,
		TopLevelIndex: topIndex,
	})

	if err != nil {
		return nil, err
	}

	return &dagstoreWrapper{dagst: dagst, indexRepo: irepo, topIndex: topIndex}, nil
}
