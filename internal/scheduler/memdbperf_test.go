package scheduler

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type IntObject struct {
	Id int
}

type ByteObject struct {
	Id []byte
}

type ByteObjectIdIndex struct{}

// FromArgs computes the index key from a set of arguments.
// Takes a single argument resourceAmount of type resource.Quantity.
func (index *ByteObjectIdIndex) FromArgs(args ...any) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	key := args[0].([]byte)
	return key, nil
}

// FromObject extracts the index key from a ByteObject.
func (index *ByteObjectIdIndex) FromObject(raw any) (bool, []byte, error) {
	obj, ok := raw.(ByteObject)
	if !ok {
		return false, nil, errors.Errorf("expected ByteObject, but got %T", raw)
	}
	return true, obj.Id, nil
}

func BenchmarkInts(b *testing.B) {
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"ints": {
				Name: "ints",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:   "id",
						Unique: true,
						Indexer: &memdb.IntFieldIndex{
							Field: "Id",
						},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(schema)
	require.NoError(b, err)

	n := 100
	objs := make([]IntObject, n)
	for i := 0; i < n; i++ {
		objs[i] = IntObject{Id: i}
	}
	txn := db.Txn(true)
	for _, obj := range objs {
		err := txn.Insert("ints", obj)
		require.NoError(b, err)
	}
	txn.Commit()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.Txn(true)
		for _, obj := range objs {
			err := txn.Insert("ints", obj)
			require.NoError(b, err)
		}
		txn.Commit()
	}
}

func BenchmarkBytes(b *testing.B) {
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"objs": {
				Name: "objs",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &ByteObjectIdIndex{},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(schema)
	require.NoError(b, err)

	n := 1000
	objs := make([]ByteObject, n)
	for i := 0; i < n; i++ {
		objs[i] = ByteObject{Id: []byte(fmt.Sprintf("%d", i))}
	}
	txn := db.Txn(true)
	for _, obj := range objs {
		err := txn.Insert("objs", obj)
		require.NoError(b, err)
	}
	// txn.Commit()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// txn := db.Txn(true)
		for _, obj := range objs {
			txn.Insert("objs", obj)
			// err := txn.Insert("objs", obj)
			// require.NoError(b, err)
		}
		// txn.Commit()
	}
}

func BenchmarkIntsMap(b *testing.B) {
	n := 1000
	objs := make([]IntObject, n)
	for i := 0; i < n; i++ {
		objs[i] = IntObject{Id: i}
	}

	m := make(map[int]IntObject)
	for _, obj := range objs {
		m[obj.Id] = obj
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, obj := range objs {
			m[obj.Id] = obj
		}
	}
}

// func BenchmarkBytesMap(b *testing.B) {
// 	n := 100
// 	objs := make([]ByteObject, n)
// 	for i := 0; i < n; i++ {
// 		objs[i] = ByteObject{Id: []byte(fmt.Sprintf("%d", i))}
// 	}

// 	m := make(map[int]IntObject)
// 	for _, obj := range objs {
// 		m[obj.Id] = obj
// 	}

// 	b.ResetTimer()
// 	for n := 0; n < b.N; n++ {
// 		for _, obj := range objs {
// 			m[obj.Id] = obj
// 		}
// 	}
// }
