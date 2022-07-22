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

package middleware

import (
	"context"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"strconv"
)

// There is no need to handle metrics configuration in the interceptors themselves, because they are not
// loaded if the metrics are not enabled. It is more efficient this way, but if we want dynamic reloading,
// we may want to change this in the future.

func getOkMethodTags(ctx context.Context, methodName string, methodType string) map[string]string {
	tags := metrics.GetPreinitializedTagsFromFullMethod(methodName, methodType)
	namespace, err := request.GetNamespace(ctx)
	if ulog.E(err) && err == request.ErrNamespaceNotFound {
		tags["tigris_tenant"] = metrics.DefaultReportedTigrisTenant
	} else {
		tags["tigris_tenant"] = namespace
	}
	return tags
}

func getErrorMethodTags(ctx context.Context, fullMethod string, methodType string, errSource string, errCode string) map[string]string {
	metaData := metrics.GetGrpcEndPointMetadataFromFullMethod(fullMethod, methodType)
	tags := metaData.GetSpecificErrorTags(errSource, errCode)
	namespace, err := request.GetNamespace(ctx)
	if ulog.E(err) && err == request.ErrNamespaceNotFound {
		tags["tigris_tenant"] = metrics.DefaultReportedTigrisTenant
	} else {
		tags["tigris_tenant"] = namespace
	}
	return tags
}

func countOkMessage(ctx context.Context, fullMethod string, methodType string) {
	// tigris_server_requests_ok
	tags := getOkMethodTags(ctx, fullMethod, methodType)
	metrics.Requests.Tagged(tags).Counter("ok").Inc(1)
}

func countUnknownErrorMessage(ctx context.Context, fullMethod string, methodType string) {
	// tigris_server_requests_error
	tags := getOkMethodTags(ctx, fullMethod, methodType)
	metrics.ErrorRequests.Tagged(tags).Counter("unknown").Inc(1)
}

func countSpecificErrorMessage(ctx context.Context, fullMethod string, methodType, errSource string, errCode string) {
	// tigris_server_requests_error
	// For specific errors the tags are not pre-initialized because it has the error code in it
	tags := getErrorMethodTags(ctx, fullMethod, methodType, errSource, errCode)
	metrics.ErrorRequests.Tagged(tags).Counter("specific").Inc(1)
}

func metricsUnaryServerInterceptorResponseTime() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		methodType := "unary"
		tags := getOkMethodTags(ctx, info.FullMethod, methodType)
		defer metrics.RequestsRespTime.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func metricsUnaryServerInterceptorCounters() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		methodType := "unary"
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(ctx, info.FullMethod, methodType, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(ctx, info.FullMethod, methodType, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(ctx, info.FullMethod, methodType)
			}
		} else {
			countOkMessage(ctx, info.FullMethod, methodType)
		}
		return resp, err
	}
}

func metricsStreamServerInterceptorResponseTime() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		methodType := "stream"
		ctx := stream.Context()
		tags := getOkMethodTags(ctx, info.FullMethod, methodType)
		defer metrics.RequestsRespTime.Tagged(tags).Histogram("histogram", tally.DefaultBuckets).Start().Stop()
		wrapper := &recvWrapper{stream}
		err := handler(srv, wrapper)
		return err
	}
}

func metricsStreamServerInterceptorCounter() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapper := &recvWrapper{stream}
		ctx := stream.Context()
		err := handler(srv, wrapper)
		methodType := "stream"
		if err != nil {
			var terr *api.TigrisError
			var ferr fdb.Error
			if errors.As(err, &terr) {
				countSpecificErrorMessage(ctx, info.FullMethod, methodType, "tigris_server", terr.Code.String())
			} else if errors.As(err, &ferr) {
				countSpecificErrorMessage(ctx, info.FullMethod, methodType, "fdb_server", strconv.Itoa(ferr.Code))
			} else {
				countUnknownErrorMessage(ctx, info.FullMethod, methodType)
			}
		} else {
			countOkMessage(ctx, info.FullMethod, methodType)
		}
		return err
	}
}
