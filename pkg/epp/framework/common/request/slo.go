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
	"strconv"
	"strings"
	"time"
)

const (
	// SLOTTFTHeaderKey is the request header for SLO time-to-first-token in milliseconds.
	SLOTTFTHeaderKey = "x-slo-ttft-ms"
)

// ParseSLODeadline computes the TTFT SLO deadline from request headers.
// Returns the deadline (receivedTimestamp + x-slo-ttft-ms) and true if valid,
// or zero time and false if the header is missing, empty, or invalid.
func ParseSLODeadline(headers map[string]string, receivedTimestamp time.Time) (time.Time, bool) {
	sloStr := GetHeader(headers, SLOTTFTHeaderKey)
	if sloStr == "" {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(sloStr), 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, false
	}
	return receivedTimestamp.Add(time.Duration(ms) * time.Millisecond), true
}
