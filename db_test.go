package mydb

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

func clean(t *testing.T, path string) {
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	} else {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNew(t *testing.T) {
	path := "data/new_test.db"
	clean(t, path)

	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	expectToBe(t, header{version: formatVersion}, *(*header)(unsafe.Pointer(&data[0])))
}

func TestPut(t *testing.T) {
	path := "data/put_test.db"
	clean(t, path)

	getUnixTime = func() int64 {
		return 1653439942
	}

	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	meta1 := metadata{offset: int64(sizeOfHeader),
		keySize:   len([]byte("key1")),
		valueSize: len([]byte("value1")),
		created:   getUnixTime(),
	}
	expectToBe(t, meta1, *(*metadata)(unsafe.Pointer(&data[sizeOfHeader])))

	expectToBe(t, []byte("key1"), (*(*[4]byte)(unsafe.Pointer(&data[meta1.offset+int64(sizeOfMetadata)])))[:])
	expectToBe(t, []byte("value1"), (*(*[6]byte)(unsafe.Pointer(&data[meta1.offset+int64(sizeOfMetadata)+int64(meta1.keySize)])))[:])

	m2Offset := meta1.offset + int64(sizeOfMetadata) + int64(meta1.keySize) + int64(meta1.valueSize)
	meta2 := metadata{offset: m2Offset,
		keySize:   len([]byte("key2")),
		valueSize: len([]byte("value2")),
		created:   getUnixTime(),
	}
	expectToBe(t, meta2, *(*metadata)(unsafe.Pointer(&data[m2Offset])))

	expectToBe(t, []byte("key2"), (*(*[4]byte)(unsafe.Pointer(&data[meta2.offset+int64(sizeOfMetadata)])))[:])
	expectToBe(t, []byte("value2"), (*(*[6]byte)(unsafe.Pointer(&data[meta2.offset+int64(sizeOfMetadata)+int64(meta2.keySize)])))[:])
}

func expectToBe(t *testing.T, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		t.Fatalf("did not match. wont: %v, got: %v", exp, act)
	}
}

func TestGet(t *testing.T) {
	path := "data/get_test.db"
	clean(t, path)

	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if err := db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	value1, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, []byte("value1"), value1)

	value2, err := db.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, []byte("value2"), value2)

	_, err = db.Get([]byte("key3"))
	expectToBe(t, ErrKeyNotFound, err)
}

func TestDelete(t *testing.T) {
	path := "data/delete_test.db"
	clean(t, path)

	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if err := db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Get([]byte("key1"))
	expectToBe(t, ErrKeyNotFound, err)

	value2, err := db.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, []byte("value2"), value2)
}

func TestNewFromExistingFile(t *testing.T) {
	path := "data/new_from_existing_file_test.db"
	clean(t, path)

	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	value1, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, []byte("value1"), value1)

	value2, err := db.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, []byte("value2"), value2)

	values, err := db.GetAll()
	if err != nil {
		t.Fatal(err)
	}
	expectToBe(t, [][]byte{[]byte("value1"), []byte("value2")}, values)
}
