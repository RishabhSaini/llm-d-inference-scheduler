/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filtering

import (
	"encoding/json"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

// AcceptAllFilterType admits all in-flight requests into the eviction queue regardless of priority.
const AcceptAllFilterType = "accept-all-eviction-filter"

func init() {
	plugin.Register(AcceptAllFilterType, AcceptAllFilterFactory)
}

// AcceptAllFilterFactory creates an AcceptAllFilter plugin.
func AcceptAllFilterFactory(name string, _ json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	f := &AcceptAllFilter{name: AcceptAllFilterType}
	if name != "" {
		f.name = name
	}
	return f, nil
}

// AcceptAllFilter admits all in-flight requests into the eviction queue.
// Use SheddableFilter for production deployments that should only evict priority < 0 requests.
type AcceptAllFilter struct {
	name string
}

var _ flowcontrol.EvictionFilterPolicy = &AcceptAllFilter{}

func (f *AcceptAllFilter) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: AcceptAllFilterType, Name: f.name}
}

func (f *AcceptAllFilter) Accept(_ *flowcontrol.EvictionItem) bool { return true }
