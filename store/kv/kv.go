// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"strconv"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
)

type KeyValue struct {
	Key    Key
	FDBKey []byte
	Data   *internal.TableData
}

type KV interface {
	Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error
	Delete(ctx context.Context, table []byte, key Key) error
	DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) error
	Read(ctx context.Context, table []byte, key Key) (Iterator, error)
	ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error)
	Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error)
	SetVersionstampedValue(ctx context.Context, key []byte, value []byte) error
	SetVersionstampedKey(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
}

type Tx interface {
	KV
	Commit(context.Context) error
	Rollback(context.Context) error
	IsRetriable() bool
}

type KeyValueStore interface {
	KV
	BeginTx(ctx context.Context) (Tx, error)
	CreateTable(ctx context.Context, name []byte) error
	DropTable(ctx context.Context, name []byte) error
	GetInternalDatabase() (interface{}, error) // TODO: CDC remove workaround
}

type Iterator interface {
	Next(value *KeyValue) bool
	Err() error
}

type KeyValueStoreImpl struct {
	*fdbkv
}

type KeyValueStoreImplWithMetrics struct {
	kv KeyValueStore
}

func NewKeyValueStore(cfg *config.FoundationDBConfig) (KeyValueStore, error) {
	kv, err := newFoundationDB(cfg)
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreImpl{fdbkv: kv}, nil
}

func NewKeyValueStoreWithMetrics(cfg *config.FoundationDBConfig) (KeyValueStore, error) {
	kv, err := newFoundationDB(cfg)
	if err != nil {
		return nil, err
	}
	return &KeyValueStoreImplWithMetrics{
		&KeyValueStoreImpl{
			fdbkv: kv,
		},
	}, nil
}

func measureLow(ctx context.Context, name string, f func() error) {
	// Low level measurement wrapper that is called by the measure functions on the appropriate receiver
	tags := metrics.GetFdbTags(ctx, name)
	if config.DefaultConfig.Metrics.Fdb.ResponseTime {
		metrics.FdbRequests.Tagged(tags).Histogram("histogram", tally.DefaultBuckets)
		defer metrics.FdbRequests.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
	}
	if config.DefaultConfig.DatadogTrace.Enabled {
		parentSpan, exists := tracer.SpanFromContext(ctx)
		if exists {
			childSpan := tracer.StartSpan(name, tracer.ChildOf(parentSpan.Context()))
			defer childSpan.Finish()
			childSpan.SetTag("fdb_op_name", name)
		}

	}
	err := f()
	if err == nil {
		// Request was ok
		metrics.FdbRequests.Tagged(tags).Counter("ok").Inc(1)
		return
	}
	var fdberr fdb.Error
	if config.DefaultConfig.Metrics.Fdb.Counters {
		errTags := metrics.GetFdbSpecificErrorTags(ctx, name, strconv.Itoa(fdberr.Code))
		// An error is either known or unknown, tagging is different, known errors have more rich metadata
		if errors.As(err, &fdberr) {
			// Error with a specific FDB error code
			metrics.FdbErrorRequests.Tagged(errTags).Counter("specific").Inc(1)
		} else {
			// Unknown error
			metrics.FdbErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
		}
	}
}

func (m *KeyValueStoreImplWithMetrics) measure(ctx context.Context, name string, f func() error) {
	measureLow(ctx, name, f)
}

func (m *KeyValueStoreImplWithMetrics) Delete(ctx context.Context, table []byte, key Key) (err error) {
	m.measure(ctx, "Delete", func() error {
		err = m.kv.Delete(ctx, table, key)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) (err error) {
	m.measure(ctx, "DeleteRange", func() error {
		err = m.kv.DeleteRange(ctx, table, lKey, rKey)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) CreateTable(ctx context.Context, name []byte) (err error) {
	m.measure(ctx, "CreateTable", func() error {
		err = m.kv.CreateTable(ctx, name)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) DropTable(ctx context.Context, name []byte) (err error) {
	m.measure(ctx, "DropTable", func() error {
		err = m.kv.DropTable(ctx, name)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedValue", func() error {
		err = m.kv.SetVersionstampedValue(ctx, key, value)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedKey", func() error {
		err = m.kv.SetVersionstampedKey(ctx, key, value)
		return err
	})
	return
}

func (m *KeyValueStoreImplWithMetrics) Get(ctx context.Context, key []byte) (val []byte, err error) {
	m.measure(ctx, "Get", func() error {
		val, err = m.kv.Get(ctx, key)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return k.fdbkv.Insert(ctx, table, key, enc)
}

func (m *KeyValueStoreImplWithMetrics) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) (err error) {
	// Whatever parameters can be passed to measure before the func
	m.measure(ctx, "Insert", func() error {
		err = m.kv.Insert(ctx, table, key, data)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return k.fdbkv.Replace(ctx, table, key, enc)
}

func (m *KeyValueStoreImplWithMetrics) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Replace", func() error {
		err = m.kv.Replace(ctx, table, key, data)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iter, err := k.fdbkv.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (m *KeyValueStoreImplWithMetrics) Read(ctx context.Context, table []byte, key Key) (it Iterator, err error) {
	m.measure(ctx, "Read", func() error {
		it, err = m.kv.Read(ctx, table, key)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error) {
	iter, err := k.fdbkv.ReadRange(ctx, table, lkey, rkey)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (m *KeyValueStoreImplWithMetrics) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (it Iterator, err error) {
	m.measure(ctx, "ReadRange", func() error {
		it, err = m.kv.ReadRange(ctx, table, lkey, rkey)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return k.fdbkv.Update(ctx, table, key, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (m *KeyValueStoreImplWithMetrics) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (encoded int32, err error) {
	m.measure(ctx, "Update", func() error {
		encoded, err = m.kv.Update(ctx, table, key, apply)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return k.fdbkv.UpdateRange(ctx, table, lKey, rKey, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (m *KeyValueStoreImplWithMetrics) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (encoded int32, err error) {
	m.measure(ctx, "UpdateRange", func() error {
		encoded, err = m.kv.UpdateRange(ctx, table, lKey, rKey, apply)
		return err
	})
	return
}

func (k *KeyValueStoreImpl) BeginTx(ctx context.Context) (Tx, error) {
	btx, err := k.fdbkv.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &TxImpl{
		ftx: btx.(*ftx),
	}, nil
}

func (m *KeyValueStoreImplWithMetrics) BeginTx(ctx context.Context) (Tx, error) {
	// This needs to be a special case in order to have the tx metrics as well
	var btx Tx
	var err error
	m.measure(ctx, "BeginTx", func() error {
		btx, err = m.kv.BeginTx(ctx)
		return err
	})
	return &TxImplWithMetrics{
		btx,
	}, err

}

func (k *KeyValueStoreImpl) GetInternalDatabase() (interface{}, error) {
	return k.db, nil
}

func (m *KeyValueStoreImplWithMetrics) GetInternalDatabase() (k interface{}, err error) {
	k, err = m.kv.GetInternalDatabase()
	return
}

type TxImpl struct {
	*ftx
}

type TxImplWithMetrics struct {
	tx Tx
}

func (m *TxImplWithMetrics) measure(ctx context.Context, name string, f func() error) {
	measureLow(ctx, name, f)
}

func (m *TxImplWithMetrics) Delete(ctx context.Context, table []byte, key Key) (err error) {
	m.measure(ctx, "Delete", func() error {
		err = m.tx.Delete(ctx, table, key)
		return err
	})
	return
}

func (m *TxImplWithMetrics) DeleteRange(ctx context.Context, table []byte, lKey Key, rKey Key) (err error) {
	m.measure(ctx, "DeleteRange", func() error {
		err = m.tx.DeleteRange(ctx, table, lKey, rKey)
		return err
	})
	return
}

func (m *TxImplWithMetrics) SetVersionstampedValue(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedValue", func() error {
		err = m.tx.SetVersionstampedValue(ctx, key, value)
		return err
	})
	return
}

func (m *TxImplWithMetrics) SetVersionstampedKey(ctx context.Context, key []byte, value []byte) (err error) {
	m.measure(ctx, "SetVersionstampedKey", func() error {
		err = m.tx.SetVersionstampedKey(ctx, key, value)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Get(ctx context.Context, key []byte) (val []byte, err error) {
	m.measure(ctx, "Get", func() error {
		val, err = m.tx.Get(ctx, key)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Commit(ctx context.Context) (err error) {
	m.measure(ctx, "Commit", func() error {
		err = m.tx.Commit(ctx)
		return err
	})
	return
}

func (m *TxImplWithMetrics) Rollback(ctx context.Context) (err error) {
	m.measure(ctx, "Rollback", func() error {
		err = m.tx.Rollback(ctx)
		return err
	})
	return
}

func (m *TxImplWithMetrics) IsRetriable() bool {
	return m.tx.IsRetriable()
}

func (tx *TxImpl) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Insert(ctx, table, key, enc)
}

func (m *TxImplWithMetrics) Insert(ctx context.Context, table []byte, key Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Insert", func() error {
		err = m.tx.Insert(ctx, table, key, data)
		return err
	})
	return
}

func (tx *TxImpl) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) error {
	enc, err := internal.Encode(data)
	if err != nil {
		return err
	}

	return tx.ftx.Replace(ctx, table, key, enc)
}

func (m *TxImplWithMetrics) Replace(ctx context.Context, table []byte, key Key, data *internal.TableData) (err error) {
	m.measure(ctx, "Replace", func() error {
		err = m.tx.Replace(ctx, table, key, data)
		return err
	})
	return
}

func (tx *TxImpl) Read(ctx context.Context, table []byte, key Key) (Iterator, error) {
	iter, err := tx.ftx.Read(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (m *TxImplWithMetrics) Read(ctx context.Context, table []byte, key Key) (it Iterator, err error) {
	m.measure(ctx, "Read", func() error {
		it, err = m.tx.Read(ctx, table, key)
		return err
	})
	return
}

func (tx *TxImpl) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (Iterator, error) {
	iter, err := tx.ftx.ReadRange(ctx, table, lkey, rkey)
	if err != nil {
		return nil, err
	}
	return &IteratorImpl{
		baseIterator: iter,
	}, nil
}

func (m *TxImplWithMetrics) ReadRange(ctx context.Context, table []byte, lkey Key, rkey Key) (it Iterator, err error) {
	m.measure(ctx, "ReadRange", func() error {
		it, err = m.tx.ReadRange(ctx, table, lkey, rkey)
		return err
	})
	return
}

func (tx *TxImpl) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return tx.ftx.Update(ctx, table, key, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (m *TxImplWithMetrics) Update(ctx context.Context, table []byte, key Key, apply func(*internal.TableData) (*internal.TableData, error)) (encoded int32, err error) {
	m.measure(ctx, "Update", func() error {
		encoded, err = m.tx.Update(ctx, table, key, apply)
		return err
	})
	return
}

func (tx *TxImpl) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (int32, error) {
	return tx.ftx.UpdateRange(ctx, table, lKey, rKey, func(existing []byte) ([]byte, error) {
		decoded, err := internal.Decode(existing)
		if err != nil {
			return nil, err
		}

		newData, err := apply(decoded)
		if err != nil {
			return nil, err
		}

		encoded, err := internal.Encode(newData)
		if err != nil {
			return nil, err
		}

		return encoded, nil
	})
}

func (m *TxImplWithMetrics) UpdateRange(ctx context.Context, table []byte, lKey Key, rKey Key, apply func(*internal.TableData) (*internal.TableData, error)) (encoded int32, err error) {
	m.measure(ctx, "UpdateRange", func() error {
		encoded, err = m.tx.UpdateRange(ctx, table, lKey, rKey, apply)
		return err
	})
	return
}

type IteratorImpl struct {
	baseIterator
	err error
}

func (i *IteratorImpl) Next(value *KeyValue) bool {
	var v baseKeyValue
	hasNext := i.baseIterator.Next(&v)
	if hasNext {
		value.Key = v.Key
		value.FDBKey = v.FDBKey
		decoded, err := internal.Decode(v.Value)
		if err != nil {
			i.err = err
			return false
		}
		value.Data = decoded
	}
	return hasNext
}

func (i *IteratorImpl) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.baseIterator.Err()
}

type KeyPart interface{}
type Key []KeyPart

func BuildKey(parts ...interface{}) Key {
	ptr := unsafe.Pointer(&parts)
	return *(*Key)(ptr)
}

func (k *Key) AddPart(part interface{}) {
	*k = append(*k, KeyPart(part))
}
