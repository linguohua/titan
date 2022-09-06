package stores

import (
	"os"
	"path/filepath"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/node/fsutil"
)

type fileStore struct {
	Path string
}

func (fs *fileStore) Type() string {
	return "FileStore"
}

func (fs *fileStore) Put(key string, value []byte) error {
	filePath := filepath.Join(fs.Path, key)
	return os.WriteFile(filePath, value, 0644)
}

func (fs *fileStore) Get(key string) ([]byte, error) {
	filePath := filepath.Join(fs.Path, key)

	data, err := os.ReadFile(filePath)
	if err != nil && os.IsNotExist(err) {
		return nil, datastore.ErrNotFound
	}

	return data, err
}

func (fs *fileStore) Delete(key string) error {
	filePath := filepath.Join(fs.Path, key)

	err := os.Remove(filePath)
	if err != nil && os.IsNotExist(err) {
		return datastore.ErrNotFound
	}

	return err
}

func (fs *fileStore) GetReader(key string) (BlockReader, error) {
	filePath := filepath.Join(fs.Path, key)
	file, err := os.Open(filePath)
	if err != nil && os.IsNotExist(err) {
		err = datastore.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	return &FileReader{file}, nil
}

func (fs *fileStore) Has(key string) (exists bool, err error) {
	filePath := filepath.Join(fs.Path, key)
	_, err = os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (fs *fileStore) GetSize(key string) (size int, err error) {
	filePath := filepath.Join(fs.Path, key)
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return int(info.Size()), nil
}

func (fs *fileStore) Stat() (fsutil.FsStat, error) {
	return fsutil.Statfs(fs.Path)
}

func (fs *fileStore) DiskUsage() (int64, error) {
	si, err := fsutil.FileSize(fs.Path)
	if err != nil {
		return 0, err
	}
	return si.OnDisk, nil
}

func (fs *fileStore) KeyCount() (int, error) {
	dir, err := os.Open(fs.Path)
	if err != nil {
		return 0, err
	}
	defer dir.Close() //nolint:errcheck

	files, err := dir.Readdir(-1)
	if err != nil {
		return 0, err
	}

	return len(files), nil
}

func (fs *fileStore) GetAllKeys() ([]string, error) {
	dir, err := os.Open(fs.Path)
	if err != nil {
		return []string{}, err
	}
	defer dir.Close() //nolint:errcheck

	files, err := dir.Readdir(-1)
	if err != nil {
		return []string{}, err
	}

	keys := make([]string, 0, len(files))
	for _, file := range files {
		keys = append(keys, file.Name())
	}

	return keys, nil
}

type FileReader struct {
	file *os.File
}

func (r *FileReader) Read(p []byte) (n int, err error) {
	return r.file.Read(p)
}

func (r *FileReader) Close() error {
	return r.file.Close()
}

func (r *FileReader) Seek(offset int64, whence int) (int64, error) {
	return r.file.Seek(offset, whence)
}

func (r *FileReader) Size() int64 {
	stat, err := r.file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}
