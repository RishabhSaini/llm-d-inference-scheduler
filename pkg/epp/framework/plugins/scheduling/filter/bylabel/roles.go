package bylabel

import (
	"encoding/json"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

const (
	// RoleLabel name
	RoleLabel = "llm-d.ai/role"

	// RoleDecode set for designated decode workers
	RoleDecode = "decode"
	// RolePrefill set for designated prefill workers
	RolePrefill = "prefill"
	// RoleEncode set for designated encode workers (first stage in pipeline)
	RoleEncode = "encode"

	// RolePrefillDecode set for workers that can handle prefill and decode stages
	RolePrefillDecode = "prefill-decode"
	// RoleEncodePrefill set for workers that can handle encode+prefill (EP/D or P/D disaggregation)
	RoleEncodePrefill = "encode-prefill"
	// RoleEncodePrefillDecode set for workers that can handle encode+prefill+decode
	RoleEncodePrefillDecode = "encode-prefill-decode"

	// RoleBoth set for workers that can act as both prefill and decode.
	//
	// Deprecated: Use RolePrefillDecode instead. This constant is maintained for backward compatibility.
	RoleBoth = "both"

	// DecodeRoleType is the type of the DecodeFilter
	DecodeRoleType = "decode-filter"
	// PrefillRoleType is the type of the PrefillFilter
	PrefillRoleType = "prefill-filter"
	// EncodeRoleType is the type of the EncodeFilter
	EncodeRoleType = "encode-filter"

	// RoleTalker set for designated talker workers
	RoleTalker = "talker"
	// RoleCode2Wav set for designated code2wav workers
	RoleCode2Wav = "code2wav"

	// TalkerRoleType is the type of the TalkerFilter
	TalkerRoleType = "talker-filter"
	// Code2WavRoleType is the type of the Code2WavFilter
	Code2WavRoleType = "code2wav-filter"
)

// DecodeRoleFactory defines the factory function for the Decode filter.
func DecodeRoleFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewDecodeRole().WithName(name), nil
}

// NewDecodeRole creates and returns an instance of the Filter configured for decode role.
func NewDecodeRole() *ByLabel {
	return NewByLabel(DecodeRoleType, RoleLabel, true, RoleDecode, RolePrefillDecode, RoleBoth, RoleEncodePrefillDecode)
}

// PrefillRoleFactory defines the factory function for the Prefill filter.
func PrefillRoleFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewPrefillRole().WithName(name), nil
}

// NewPrefillRole creates and returns an instance of the Filter configured for prefill role.
func NewPrefillRole() *ByLabel {
	return NewByLabel(PrefillRoleType, RoleLabel, false, RolePrefill, RoleEncodePrefill, RolePrefillDecode, RoleBoth, RoleEncodePrefillDecode)
}

// EncodeRoleFactory defines the factory function for the Encode filter.
func EncodeRoleFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewEncodeRole().WithName(name), nil
}

// NewEncodeRole creates and returns an instance of the Filter configured for encode role.
// Encode is the first stage in the pipeline: Encode -> Prefill -> Decode.
// Accepts pods with roles: encode, encode-prefill, or encode-prefill-decode.
func NewEncodeRole() *ByLabel {
	return NewByLabel(EncodeRoleType, RoleLabel, false, RoleEncode, RoleEncodePrefill, RoleEncodePrefillDecode)
}

// TalkerRoleFactory defines the factory function for the Talker filter.
func TalkerRoleFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewTalkerRole().WithName(name), nil
}

// NewTalkerRole creates and returns an instance of the Filter configured for talker role.
func NewTalkerRole() *ByLabel {
	return NewByLabel(TalkerRoleType, RoleLabel, false, RoleTalker)
}

// Code2WavRoleFactory defines the factory function for the Code2Wav filter.
func Code2WavRoleFactory(name string, _ *json.Decoder, _ plugin.Handle) (plugin.Plugin, error) {
	return NewCode2WavRole().WithName(name), nil
}

// NewCode2WavRole creates and returns an instance of the Filter configured for code2wav role.
func NewCode2WavRole() *ByLabel {
	return NewByLabel(Code2WavRoleType, RoleLabel, false, RoleCode2Wav)
}
