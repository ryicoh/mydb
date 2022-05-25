package mydb

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"
	"unsafe"
)

const formatVersion uint8 = 1

var getUnixTime = func() int64 {
	return time.Now().Unix()
}

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrInvalidFormat = errors.New("invalid format")
)

type (
	DB struct {
		file   *os.File
		d      map[string]*metadata
		size   int64
		cursor int64
	}

	header struct {
		version uint8
	}

	metadata struct {
		offset    int64
		keySize   int
		valueSize int
		created   int64
		deleted   int64
	}
)

const sizeOfHeader = unsafe.Sizeof(header{})
const sizeOfMetadata = unsafe.Sizeof(metadata{})

func New(path string) (*DB, error) {
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}

		h := &header{version: formatVersion}
		bytes := (*(*[sizeOfHeader]byte)(unsafe.Pointer(h)))[:]
		if _, err := file.Write(bytes); err != nil {
			return nil, err
		}

		return &DB{
			file:   file,
			d:      make(map[string]*metadata),
			size:   0,
			cursor: int64(sizeOfHeader),
		}, nil
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, int(sizeOfHeader))
	if _, err := file.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	h := *((*header)(unsafe.Pointer(&buf[0])))
	if h.version != formatVersion {
		return nil, ErrInvalidFormat
	}

	d := make(map[string]*metadata)

	buf = make([]byte, int(sizeOfMetadata))
	cursor := int(sizeOfHeader)
	for {
		buf = make([]byte, int(sizeOfMetadata))
		_, err := file.ReadAt(buf, int64(cursor))
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		cursor += int(sizeOfMetadata)
		meta := (*metadata)(unsafe.Pointer(&buf[0]))
		if meta.deleted != 0 {
			cursor += meta.keySize + meta.valueSize
			continue
		}
		key := make([]byte, meta.keySize)
		_, err = file.ReadAt(key, int64(cursor))
		if err != nil {
			return nil, err
		}

		cursor += int(meta.keySize) + int(meta.valueSize)

		d[string(key)] = meta
	}

	db := &DB{file: file, d: d}
	return db, nil
}

func (d *DB) Close() error {
	if err := d.file.Close(); err != nil {
		return err
	}

	return nil
}

func (d *DB) Put(key, value []byte) error {
	meta := &metadata{
		offset:    d.cursor,
		keySize:   len(key),
		valueSize: len(value),
		created:   getUnixTime(),
	}

	size := int64(int(sizeOfMetadata) + meta.keySize + meta.valueSize)
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.Write((*(*[sizeOfMetadata]byte)(unsafe.Pointer(meta)))[:])
	buf.Write(key)
	buf.Write(value)
	if _, err := d.file.WriteAt(buf.Bytes(), d.cursor); err != nil {
		return err
	}
	d.cursor += size

	d.d[string(key)] = meta
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	meta, ok := d.d[string(key)]
	if !ok || meta.deleted != 0 {
		return nil, ErrKeyNotFound
	}

	value := make([]byte, meta.valueSize)
	offset := meta.offset + int64(sizeOfMetadata) + int64(meta.keySize)
	_, err := d.file.ReadAt(value, offset)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (d *DB) Delete(key []byte) error {
	keyStr := string(key)
	meta, ok := d.d[keyStr]
	if !ok || meta.deleted != 0 {
		return ErrKeyNotFound
	}

	meta.deleted = getUnixTime()

	buf := (*(*[sizeOfMetadata]byte)(unsafe.Pointer(meta)))[:]
	if _, err := d.file.WriteAt(buf, meta.offset); err != nil {
		return err
	}

	return nil
}

func (d *DB) GetAll() ([][]byte, error) {
	values := make([][]byte, 0, len(d.d))
	for _, meta := range d.d {
		if meta.deleted != 0 {
			return nil, ErrKeyNotFound
		}

		value := make([]byte, meta.valueSize)
		offset := meta.offset + int64(sizeOfMetadata) + int64(meta.keySize)
		_, err := d.file.ReadAt(value, offset)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	return values, nil
}
