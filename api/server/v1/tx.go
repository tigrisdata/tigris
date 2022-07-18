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

package api

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func IsTxSupported(ctx context.Context) bool {
	m, _ := grpc.Method(ctx)
	switch m {
	case "Insert", "Replace", "Update", "Delete", "Read",
		"CreateOrUpdateCollection", "DropCollection", "ListCollections",
		"CommitTransaction", "RollbackTransaction", "Publish":
		return true
	default:
		return false
	}
}

func GetTransaction(ctx context.Context, req proto.Message) *TransactionCtx {
	tx := &TransactionCtx{
		Id:     GetHeader(ctx, HeaderTxID),
		Origin: GetHeader(ctx, HeaderTxOrigin),
	}

	if tx.Id != "" {
		return tx
	}

	return GetTransactionLegacy(req)
}

func GetTransactionLegacy(req proto.Message) *TransactionCtx {
	switch r := req.(type) {
	case *InsertRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReplaceRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *UpdateRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *DeleteRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReadRequest:
		if r.GetOptions() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *CreateOrUpdateCollectionRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *DropCollectionRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *ListCollectionsRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	case *CommitTransactionRequest:
		return r.GetTxCtx()
	case *RollbackTransactionRequest:
		return r.GetTxCtx()
	case *PublishRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	}
	return nil
}
