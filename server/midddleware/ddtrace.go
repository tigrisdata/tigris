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
	"fmt"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/tigrisdata/tigris/server/request"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"strings"
)

func getMethodName(fullMethod string) string {
	return strings.Split(fullMethod, "/")[2]
}

func getServiceName(fullMethod string) string {
	return strings.Split(fullMethod, "/")[1]
}

func getSpanName(fullMethod string) string {
	return fmt.Sprintf("tigris.server.grpc.%s", getMethodName(fullMethod))
}

func getNamespaceName(ctx context.Context) string {
	namespace, err := request.GetNamespace(ctx)
	if ulog.E(err) && err == request.ErrNamespaceNotFound {
		return metrics.DefaultReportedTigrisTenant
	} else {
		return namespace
	}
}

func getTags(ctx context.Context, fullMethod string, serviceType string) map[string]string {
	return map[string]string{
		"method":            getMethodName(fullMethod),
		"tigris_tenant":     getNamespaceName(ctx),
		"grpc_service_name": getServiceName(fullMethod),
		"grpc_service_type": serviceType,
	}
}

func ddTraceUnary() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		span := tracer.StartSpan(info.FullMethod)
		defer span.Finish()
		ctx = tracer.ContextWithSpan(ctx, span)

		for k, v := range getTags(ctx, info.FullMethod, "unary") {
			span.SetTag(k, v)
		}
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func ddTraceStream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		span := tracer.StartSpan(getSpanName(info.FullMethod))
		defer span.Finish()
		ctx := tracer.ContextWithSpan(stream.Context(), span)
		for k, v := range getTags(ctx, info.FullMethod, "stream") {
			span.SetTag(k, v)
		}
		wrapper := &recvWrapper{stream}
		err := handler(srv, wrapper)
		return err
	}
}
