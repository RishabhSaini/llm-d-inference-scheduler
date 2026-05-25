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

package request

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseSLODeadline(t *testing.T) {
	t.Parallel()
	received := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		headers   map[string]string
		wantOk    bool
		wantDelta time.Duration
	}{
		{
			name:      "valid header",
			headers:   map[string]string{"x-slo-ttft-ms": "500"},
			wantOk:    true,
			wantDelta: 500 * time.Millisecond,
		},
		{
			name:      "case insensitive",
			headers:   map[string]string{"X-SLO-TTFT-MS": "200"},
			wantOk:    true,
			wantDelta: 200 * time.Millisecond,
		},
		{
			name:      "with whitespace",
			headers:   map[string]string{"x-slo-ttft-ms": " 300 "},
			wantOk:    true,
			wantDelta: 300 * time.Millisecond,
		},
		{
			name:    "missing header",
			headers: map[string]string{"other": "value"},
			wantOk:  false,
		},
		{
			name:    "empty header",
			headers: map[string]string{"x-slo-ttft-ms": ""},
			wantOk:  false,
		},
		{
			name:    "invalid number",
			headers: map[string]string{"x-slo-ttft-ms": "abc"},
			wantOk:  false,
		},
		{
			name:    "zero value",
			headers: map[string]string{"x-slo-ttft-ms": "0"},
			wantOk:  false,
		},
		{
			name:    "nil headers",
			headers: nil,
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			deadline, ok := ParseSLODeadline(tt.headers, received)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, received.Add(tt.wantDelta), deadline)
			}
		})
	}
}
