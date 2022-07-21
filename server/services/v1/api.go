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

package v1

import (
	"context"
	"net/http"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/server/cdc"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"github.com/tigrisdata/tigris/util"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
)

const (
	databasePath        = "/databases"
	databasePathPattern = databasePath + "/*"

	collectionPath        = databasePath + "/collections"
	collectionPathPattern = collectionPath + "/*"

	documentPath        = collectionPath + "/documents"
	documentPathPattern = documentPath + "/*"

	infoPath    = "/info"
	metricsPath = "/metrics"
)

type apiService struct {
	api.UnimplementedTigrisServer

	kvStore       kv.KeyValueStore
	txMgr         *transaction.Manager
	encoder       metadata.Encoder
	tenantMgr     *metadata.TenantManager
	cdcMgr        *cdc.Manager
	sessions      *SessionManager
	runnerFactory *QueryRunnerFactory
	versionH      *metadata.VersionHandler
	searchStore   search.Store
}

func newApiService(kv kv.KeyValueStore, searchStore search.Store, tenantMgr *metadata.TenantManager, txMgr *transaction.Manager) *apiService {
	u := &apiService{
		kvStore:     kv,
		txMgr:       txMgr,
		versionH:    &metadata.VersionHandler{},
		searchStore: searchStore,
	}

	ctx := context.TODO()
	tx, err := u.txMgr.StartTx(ctx)
	if ulog.E(err) {
		log.Fatal().Err(err).Msgf("error starting server: starting transaction failed")
	}

	if err := tenantMgr.Reload(ctx, tx); ulog.E(err) {
		// ToDo: no need to panic, probably handle through async thread.
		log.Err(err).Msgf("error starting server: reloading tenants failed")
	}
	_ = tx.Commit(ctx)

	u.tenantMgr = tenantMgr
	u.encoder = metadata.NewEncoder(u.tenantMgr)
	u.cdcMgr = cdc.NewManager()
	u.sessions = NewSessionManager(u.txMgr, u.tenantMgr, u.versionH, u.cdcMgr, u.searchStore, u.encoder)
	u.runnerFactory = NewQueryRunnerFactory(u.txMgr, u.encoder, u.cdcMgr, u.searchStore)
	return u
}

func (s *apiService) RegisterHTTP(router chi.Router, inproc *inprocgrpc.Channel) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{JSONBuiltin: &runtime.JSONBuiltin{}}),
		runtime.WithIncomingHeaderMatcher(api.CustomMatcher),
		runtime.WithOutgoingHeaderMatcher(api.CustomMatcher),
	)

	if err := api.RegisterTigrisHandlerClient(context.TODO(), mux, api.NewTigrisClient(inproc)); err != nil {
		return err
	}

	api.RegisterTigrisServer(inproc, s)

	router.HandleFunc(apiPathPrefix+databasePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+collectionPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+documentPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.HandleFunc(apiPathPrefix+infoPath, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	router.Handle(metricsPath, metrics.Reporter.HTTPHandler())
	return nil
}

func (s *apiService) RegisterGRPC(grpc *grpc.Server) error {
	api.RegisterTigrisServer(grpc, s)
	return nil
}

func (s *apiService) BeginTransaction(ctx context.Context, _ *api.BeginTransactionRequest) (*api.BeginTransactionResponse, error) {
	// explicit transactions needed to be tracked
	session, err := s.sessions.Create(ctx, true, true)
	if err != nil {
		return nil, err
	}

	return &api.BeginTransactionResponse{
		TxCtx: session.txCtx,
	}, nil
}

func (s *apiService) CommitTransaction(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
	txCtx := api.GetTransaction(ctx, r)
	session := s.sessions.Get(txCtx.GetId())
	if session == nil {
		return nil, api.Errorf(api.Code_NOT_FOUND, "session not found")
	}
	defer s.sessions.Remove(session.txCtx.Id)

	err := session.Commit(s.versionH, session.tx.Context().GetStagedDatabase() != nil, nil)
	if err != nil {
		return nil, err
	}

	return &api.CommitTransactionResponse{}, nil
}

func (s *apiService) RollbackTransaction(ctx context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
	txCtx := api.GetTransaction(ctx, r)
	session := s.sessions.Get(txCtx.GetId())
	if session == nil {
		return nil, api.Errorf(api.Code_NOT_FOUND, "session not found")
	}
	defer s.sessions.Remove(session.txCtx.Id)

	_ = session.Rollback()

	return &api.RollbackTransactionResponse{}, nil
}

// Insert new object returns an error if object already exists
// Operations done individually not in actual batch
func (s *apiService) Insert(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(ctx, r),
		queryRunner: s.runnerFactory.GetInsertQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.InsertResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Replace(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(ctx, r),
		queryRunner: s.runnerFactory.GetReplaceQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.ReplaceResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			CreatedAt: resp.createdAt.GetProtoTS(),
		},
		Keys: resp.allKeys,
	}, nil
}

func (s *apiService) Update(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(ctx, r),
		queryRunner: s.runnerFactory.GetUpdateQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.UpdateResponse{
		Status:        resp.status,
		ModifiedCount: resp.modifiedCount,
		Metadata: &api.ResponseMetadata{
			UpdatedAt: resp.updatedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Delete(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(ctx, r),
		queryRunner: s.runnerFactory.GetDeleteQueryRunner(r),
	})
	if err != nil {
		return nil, err
	}

	return &api.DeleteResponse{
		Status: resp.status,
		Metadata: &api.ResponseMetadata{
			DeletedAt: resp.deletedAt.GetProtoTS(),
		},
	}, nil
}

func (s *apiService) Read(r *api.ReadRequest, stream api.Tigris_ReadServer) error {
	_, err := s.sessions.Execute(stream.Context(), &ReqOptions{
		txCtx:       api.GetTransaction(stream.Context(), r),
		queryRunner: s.runnerFactory.GetStreamingQueryRunner(r, stream),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *apiService) Search(r *api.SearchRequest, stream api.Tigris_SearchServer) error {
	_, err := s.sessions.Execute(stream.Context(), &ReqOptions{
		txCtx:       api.GetTransaction(stream.Context(), r),
		queryRunner: s.runnerFactory.GetSearchQueryRunner(r, stream),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *apiService) CreateOrUpdateCollection(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetCreateOrUpdateCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:          api.GetTransaction(ctx, r),
		queryRunner:    runner,
		metadataChange: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateOrUpdateCollectionResponse{
		Status:  resp.status,
		Message: "collection created successfully",
	}, nil
}

func (s *apiService) DropCollection(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDropCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:          api.GetTransaction(ctx, r),
		queryRunner:    runner,
		metadataChange: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropCollectionResponse{
		Status:  resp.status,
		Message: "collection dropped successfully",
	}, nil
}

func (s *apiService) ListCollections(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetListCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		txCtx:       api.GetTransaction(ctx, r),
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListCollectionsResponse), nil
}

func (s *apiService) ListDatabases(ctx context.Context, r *api.ListDatabasesRequest) (*api.ListDatabasesResponse, error) {
	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetListDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner: queryRunner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.ListDatabasesResponse), nil
}

func (s *apiService) CreateDatabase(ctx context.Context, r *api.CreateDatabaseRequest) (*api.CreateDatabaseResponse, error) {
	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetCreateDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner:    queryRunner,
		metadataChange: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.CreateDatabaseResponse{
		Status:  resp.status,
		Message: "database created successfully",
	}, nil
}

func (s *apiService) DropDatabase(ctx context.Context, r *api.DropDatabaseRequest) (*api.DropDatabaseResponse, error) {
	queryRunner := s.runnerFactory.GetDatabaseQueryRunner()
	queryRunner.SetDropDatabaseReq(r)
	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner:    queryRunner,
		metadataChange: true,
	})
	if err != nil {
		return nil, err
	}

	return &api.DropDatabaseResponse{
		Status:  resp.status,
		Message: "database dropped successfully",
	}, nil
}

func (s *apiService) DescribeCollection(ctx context.Context, r *api.DescribeCollectionRequest) (*api.DescribeCollectionResponse, error) {
	runner := s.runnerFactory.GetCollectionQueryRunner()
	runner.SetDescribeCollectionReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeCollectionResponse), nil
}

func (s *apiService) DescribeDatabase(ctx context.Context, r *api.DescribeDatabaseRequest) (*api.DescribeDatabaseResponse, error) {
	runner := s.runnerFactory.GetDatabaseQueryRunner()
	runner.SetDescribeDatabaseReq(r)

	resp, err := s.sessions.Execute(ctx, &ReqOptions{
		queryRunner: runner,
	})
	if err != nil {
		return nil, err
	}

	return resp.Response.(*api.DescribeDatabaseResponse), nil
}

func (s *apiService) GetInfo(_ context.Context, _ *api.GetInfoRequest) (*api.GetInfoResponse, error) {
	return &api.GetInfoResponse{
		ServerVersion: util.Version,
	}, nil
}

func (s *apiService) Events(r *api.EventsRequest, stream api.Tigris_EventsServer) error {
	if !config.DefaultConfig.Cdc.Enabled {
		return api.Errorf(api.Code_METHOD_NOT_ALLOWED, "change streams is disabled for this collection")
	}
	publisher := s.cdcMgr.GetPublisher(r.GetDb())
	streamer, err := publisher.NewStreamer(s.kvStore)
	if err != nil {
		return err
	}
	defer streamer.Close()

	for tx := range streamer.Txs {
		for _, op := range tx.Ops {
			_, _, collection, ok := s.encoder.DecodeTableName(op.Table)
			if !ok {
				log.Err(err).Str("table", string(op.Table)).Msg("failed to decode collection name")
				return api.Errorf(api.Code_INTERNAL, "failed to decode collection name")
			}

			if r.Collection == "" || r.Collection == collection {
				td, err := internal.Decode(op.Data)
				if err != nil {
					log.Err(err).Str("data", string(op.Data)).Msg("failed to decode data")
					return api.Errorf(api.Code_INTERNAL, "failed to decode data")
				}

				event := &api.StreamEvent{
					TxId:       tx.Id,
					Collection: collection,
					Op:         op.Op,
					Key:        op.Key,
					Lkey:       op.LKey,
					Rkey:       op.RKey,
					Data:       td.RawData,
					Last:       op.Last,
				}

				response := &api.EventsResponse{
					Event: event,
				}

				if err := stream.Send(response); ulog.E(err) {
					return err
				}
			}
		}
	}

	return nil
}

func (s *apiService) Publish(ctx context.Context, r *api.PublishRequest) (*api.PublishResponse, error) {
	var wo *api.WriteOptions = nil
	if r.GetOptions() != nil {
		wo = r.GetOptions().WriteOptions
	}

	resp, err := s.Insert(ctx, &api.InsertRequest{
		Db:         r.Db,
		Collection: r.Collection,
		Documents:  [][]byte{r.Message},
		Options:    &api.InsertRequestOptions{WriteOptions: wo},
	})

	if err != nil {
		return nil, err
	}

	return &api.PublishResponse{
		Status: resp.Status,
	}, nil
}

func (s *apiService) Subscribe(r *api.SubscribeRequest, stream api.Tigris_SubscribeServer) error {
	publisher := s.cdcMgr.GetPublisher(r.GetDb())
	streamer, err := publisher.NewStreamer(s.kvStore)
	if err != nil {
		return err
	}
	defer streamer.Close()

	for tx := range streamer.Txs {
		for _, op := range tx.Ops {
			_, _, collection, ok := s.encoder.DecodeTableName(op.Table)
			if !ok {
				log.Err(err).Str("table", string(op.Table)).Msg("failed to decode collection name")
				return api.Errorf(api.Code_INTERNAL, "failed to decode collection name")
			}

			if r.Collection == collection {
				td, err := internal.Decode(op.Data)
				if err != nil {
					log.Err(err).Str("data", string(op.Data)).Msg("failed to decode data")
					return api.Errorf(api.Code_INTERNAL, "failed to decode data")
				}

				response := &api.SubscribeResponse{
					Message: td.RawData,
				}

				if err := stream.Send(response); ulog.E(err) {
					return err
				}
			}
		}
	}

	return nil
}
