package v1

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/value"
)

const (
	// generatorSubspaceKey is used to store ids in storage so that we can guarantee uniqueness
	generatorSubspaceKey = "generator"
	// int32IdKey is the prefix after generator subspace to store int32 counters
	int32IdKey = "int32_id"
)

var (
	zeroIntStringSlice  = []byte("0")
	zeroUUIDStringSlice = []byte(uuid.NullUUID.String())
	zeroTimeStringSlice = []byte(time.Time{}.Format(time.RFC3339Nano))
)

// keyGenerator is used to extract the keys from document and return keys.Key which will be used by Insert/Replace API.
// keyGenerator may mutate the document in case autoGenerate is set for primary key fields.
type keyGenerator struct {
	generator   *generator
	document    []byte
	keysForResp []byte
	index       *schema.Index
	forceInsert bool
}

func newKeyGenerator(document []byte, generator *generator, index *schema.Index) *keyGenerator {
	return &keyGenerator{
		document:  document,
		generator: generator,
		index:     index,
	}
}

func (k *keyGenerator) getKeysForResp() []byte {
	return []byte(fmt.Sprintf(`{%s}`, k.keysForResp))
}

// generate method also modifies the JSON document in case of autoGenerate primary key.
func (k *keyGenerator) generate(ctx context.Context, encoder metadata.Encoder, table []byte) (keys.Key, error) {
	var indexParts []interface{}
	for _, field := range k.index.Fields {
		jsonVal, dtp, _, err := jsonparser.Get(k.document, field.FieldName)
		autoGenerate := field.IsAutoGenerated() && (dtp == jsonparser.NotExist ||
			err == nil && (isNull(field.Type(), jsonVal) || dtp == jsonparser.Null))

		if !autoGenerate && err != nil {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, errors.Wrapf(err, "missing index key column(s) '%s'", field.FieldName).Error())
		}

		var v value.Value
		if autoGenerate {
			if jsonVal, v, err = k.generator.get(ctx, table, field); err != nil {
				return nil, err
			}
			if err = k.setKeyInDoc(field, jsonVal); err != nil {
				return nil, err
			}
			if field.Type() == schema.Int64Type || field.Type() == schema.DateTimeType {
				// if we have autogenerated pkey and if it is prone to conflict then force to use Insert API
				k.forceInsert = true
			}
		} else if v, err = value.NewValue(field.Type(), jsonVal); err != nil {
			return nil, err
		}

		k.addKeyToResp(field, jsonVal)
		indexParts = append(indexParts, v.AsInterface())
	}

	return encoder.EncodeKey(table, k.index, indexParts)
}

func (k *keyGenerator) setKeyInDoc(field *schema.Field, jsonVal []byte) error {
	jsonVal = k.getJsonQuotedValue(field.Type(), jsonVal)

	var err error
	k.document, err = jsonparser.Set(k.document, jsonVal, field.FieldName)
	return err
}

func (k *keyGenerator) addKeyToResp(field *schema.Field, jsonVal []byte) {
	jsonVal = k.getJsonQuotedValue(field.Type(), jsonVal)
	jsonKeyAndValue := []byte(fmt.Sprintf(`"%s":%s`, field.FieldName, jsonVal))

	if len(k.keysForResp) == 0 {
		k.keysForResp = jsonKeyAndValue
	} else {
		k.keysForResp = append(k.keysForResp, []byte(`,`)...)
		k.keysForResp = append(k.keysForResp, jsonKeyAndValue...)
	}
}

func (k *keyGenerator) getJsonQuotedValue(fieldType schema.FieldType, jsonVal []byte) []byte {
	switch fieldType {
	case schema.StringType, schema.UUIDType, schema.ByteType, schema.DateTimeType:
		return []byte(fmt.Sprintf(`"%s"`, jsonVal))
	default:
		return jsonVal
	}
}

type generator struct {
	txMgr *transaction.Manager
}

func newGenerator(txMgr *transaction.Manager) *generator {
	return &generator{
		txMgr: txMgr,
	}
}

// isNull checks if the value is "zero" value of it's type
func isNull(tp schema.FieldType, val []byte) bool {
	switch tp {
	case schema.Int32Type:
		return bytes.Equal(val, zeroIntStringSlice)
	case schema.Int64Type:
		return bytes.Equal(val, zeroIntStringSlice)
	case schema.UUIDType:
		return bytes.Equal(val, zeroUUIDStringSlice)
	case schema.DateTimeType:
		return bytes.Equal(val, zeroTimeStringSlice)
	case schema.StringType, schema.ByteType:
		return len(val) == 0
	}
	return false
}

// get returns generated id for the supported primary key fields. This method returns unquoted JSON values. This is to
// align with the json library that we are using as that returns unquoted strings as well. It is returning internal
// value as well so that we don't need to recalculate it from jsonVal.
func (g *generator) get(ctx context.Context, table []byte, field *schema.Field) ([]byte, value.Value, error) {
	switch field.Type() {
	case schema.StringType, schema.UUIDType:
		value := value.NewStringValue(uuid.NewUUIDAsString())
		return []byte(*value), value, nil
	case schema.ByteType:
		value := value.NewBytesValue([]byte(uuid.NewUUIDAsString()))
		b64 := base64.StdEncoding.EncodeToString([]byte(*value))
		return []byte(b64), value, nil
	case schema.DateTimeType:
		// use timestamp nano to reduce the contention if multiple workers end up generating same timestamp.
		value := value.NewStringValue(time.Now().UTC().Format(time.RFC3339Nano))
		return []byte(*value), value, nil
	case schema.Int64Type:
		// use timestamp nano to reduce the contention if multiple workers end up generating same timestamp.
		value := value.NewIntValue(time.Now().UTC().UnixNano())
		return []byte(fmt.Sprintf(`%d`, *value)), value, nil
	case schema.Int32Type:
		valueI32, err := g.generateInTx(ctx, table)
		if err != nil {
			return nil, nil, err
		}

		value := value.NewIntValue(int64(valueI32))
		return []byte(fmt.Sprintf(`%d`, *value)), value, nil
	}
	return nil, nil, api.Errorf(api.Code_INVALID_ARGUMENT, "unsupported type found in auto-generator")
}

// generateInTx is used to generate an id in a transaction for int32 field only. This is mainly used to guarantee
// uniqueness with auto-incremented ids, so what we are doing is reserving this id in storage before returning to the
// caller so that only one id is assigned to one caller.
func (g *generator) generateInTx(ctx context.Context, table []byte) (int32, error) {
	for {
		tx, err := g.txMgr.StartTx(ctx)
		if err != nil {
			return -1, err
		}

		var valueI32 int32
		if valueI32, err = g.generateInt(ctx, tx, table); err != nil {
			_ = tx.Rollback(ctx)
		}

		if err = tx.Commit(ctx); err == nil {
			return valueI32, nil
		}
		if err != kv.ErrConflictingTransaction {
			return -1, err
		}
	}
}

// generateInt as it is used to generate int32 value, we are simply maintaining a counter. There is a contention to
// generate a counter if it is concurrently getting executed but the generation should be fast then it is best to start
// with this approach.
func (g *generator) generateInt(ctx context.Context, tx transaction.Tx, table []byte) (int32, error) {
	key := keys.NewKey([]byte(generatorSubspaceKey), table, int32IdKey)
	it, err := tx.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	id := uint32(1)
	var row kv.KeyValue
	if it.Next(&row) {
		id = encoding.ByteToUInt32(row.Data.RawData) + uint32(1)
	}
	if err := it.Err(); err != nil {
		return 0, err
	}

	if err := tx.Replace(ctx, key, internal.NewTableData(encoding.UInt32ToByte(id))); err != nil {
		return 0, err
	}

	return int32(id), nil
}
