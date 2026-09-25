/*
Copyright 2025 The Kubernetes Authors.
Copyright 2026 The llm-d Authors.

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

// Package v1 contains API Schema definitions for the
// llm-d.ai API group. This version is not served yet; serving starts with
// the conversion strategy agreed for the v1 promotion. The types stay out
// of CRD generation until then.
//
// +k8s:openapi-gen=true
// +kubebuilder:object:generate=true
// +groupName=llm-d.ai
// +groupGoName=XInference
package v1
