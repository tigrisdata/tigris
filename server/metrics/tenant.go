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

package metrics

import (
	"context"
	"github.com/tigrisdata/tigris/server/request"
	ulog "github.com/tigrisdata/tigris/util/log"
)

const (
	DefaultReportedTigrisTenant string = "unknown"
)

func addTigrisTenantToTags(ctx context.Context, tags map[string]string) map[string]string {
	namespace, err := request.GetNamespace(ctx)
	if ulog.E(err) {
		tags["tigris_tenant"] = DefaultReportedTigrisTenant
	} else {
		tags["tigris_tenant"] = namespace
	}
	return tags
}
