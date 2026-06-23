package headerrouted

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	HandlerType        = "header-routed-profile-handler"
	DefaultHeaderKey   = "EPP-Phase"
	DefaultProfileName = "default"
)

var _ fwksched.ProfileHandler = &Handler{}

type handlerParameters struct {
	HeaderKey      string `json:"headerKey"`
	DefaultProfile string `json:"defaultProfile"`
}

func HandlerFactory(name string, decoder *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var params handlerParameters
	if decoder != nil {
		if err := decoder.Decode(&params); err != nil {
			return nil, fmt.Errorf("header-routed-profile-handler: decode params: %w", err)
		}
	}
	if params.HeaderKey == "" {
		params.HeaderKey = DefaultHeaderKey
	}
	if params.DefaultProfile == "" {
		params.DefaultProfile = DefaultProfileName
	}
	return &Handler{
		typedName:      fwkplugin.TypedName{Type: HandlerType, Name: name},
		headerKey:      params.HeaderKey,
		defaultProfile: params.DefaultProfile,
	}, nil
}

type Handler struct {
	typedName      fwkplugin.TypedName
	headerKey      string
	defaultProfile string
}

func (h *Handler) TypedName() fwkplugin.TypedName { return h.typedName }

func (h *Handler) WithName(name string) *Handler {
	h.typedName.Name = name
	return h
}

func (h *Handler) Pick(_ context.Context, request *fwksched.InferenceRequest, profiles map[string]fwksched.SchedulerProfile,
	profileResults map[string]*fwksched.ProfileRunResult) map[string]fwksched.SchedulerProfile {
	if len(profiles) == len(profileResults) {
		return map[string]fwksched.SchedulerProfile{}
	}

	profileName := h.defaultProfile
	if request != nil && request.Headers != nil {
		// Headers are lowercased by the ext_proc handler.
		lowerKey := strings.ToLower(h.headerKey)
		if phase, ok := request.Headers[lowerKey]; ok && phase != "" {
			profileName = phase
		}
	}

	if profile, ok := profiles[profileName]; ok {
		return map[string]fwksched.SchedulerProfile{profileName: profile}
	}

	return profiles
}

func (h *Handler) ProcessResults(_ context.Context, _ *fwksched.InferenceRequest,
	profileResults map[string]*fwksched.ProfileRunResult) (*fwksched.SchedulingResult, error) {
	var primaryName string
	for name := range profileResults {
		primaryName = name
		break
	}
	if profileResults[primaryName] == nil {
		return nil, fmt.Errorf("failed to run scheduler profile '%s'", primaryName)
	}
	return &fwksched.SchedulingResult{
		ProfileResults:     profileResults,
		PrimaryProfileName: primaryName,
	}, nil
}
