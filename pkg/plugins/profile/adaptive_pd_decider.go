package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/util/logging"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

const (
	// AdaptivePDDeciderPluginType is the type-name of the adaptive PD decider plugin.
	AdaptivePDDeciderPluginType = "adaptive-pd-decider"

	// Default configuration values
	defaultInitialPrefillThroughput  = 2000.0  // tokens/sec
	defaultInitialDecodeThroughput   = 500.0   // tokens/sec
	defaultKVTransferLatencyPerToken = 0.0001  // seconds per token
	defaultSmallRequestThreshold     = 1000    // tokens
	defaultMediumRequestThreshold    = 10000   // tokens
	defaultLargeRequestThreshold     = 50000   // tokens
	defaultThroughputWindowSize      = 50      // observations
	defaultCostMargin                = 0.1     // 10% hysteresis
	defaultMaxOutputTokens           = 2048    // conservative default when not specified
	defaultCacheOverheadFactor       = 0.05    // 5% overhead for cache operations
)

// WorkloadClass categorizes requests by total token count
type WorkloadClass int

const (
	SmallWorkload WorkloadClass = iota // <1k tokens
	MediumWorkload                      // 1k-10k tokens
	LargeWorkload                       // 10k-50k tokens
	UltraWorkload                       // >50k tokens
)

func (w WorkloadClass) String() string {
	switch w {
	case SmallWorkload:
		return "small"
	case MediumWorkload:
		return "medium"
	case LargeWorkload:
		return "large"
	case UltraWorkload:
		return "ultra"
	default:
		return "unknown"
	}
}

// AdaptivePDDeciderConfig holds the configuration for the adaptive PD decider plugin.
type AdaptivePDDeciderConfig struct {
	// Throughput estimation (initial values, self-tuning from observations)
	InitialPrefillThroughput float64 `json:"initialPrefillThroughput"` // tokens/sec (default: 2000)
	InitialDecodeThroughput  float64 `json:"initialDecodeThroughput"`  // tokens/sec (default: 500)

	// KV transfer cost
	KVTransferLatencyPerToken float64 `json:"kvTransferLatencyPerToken"` // seconds per token (default: 0.0001)

	// Workload class thresholds
	SmallRequestThreshold  int `json:"smallRequestThreshold"`  // tokens (default: 1000)
	MediumRequestThreshold int `json:"mediumRequestThreshold"` // tokens (default: 10000)
	LargeRequestThreshold  int `json:"largeRequestThreshold"`  // tokens (default: 50000)

	// Adaptation parameters
	ThroughputWindowSize int     `json:"throughputWindowSize"` // Observations for percentile calc (default: 50)
	CostMargin           float64 `json:"costMargin"`           // Safety margin for decisions, 0-1 (default: 0.1)

	// Default max output tokens when not specified in request
	DefaultMaxOutputTokens int `json:"defaultMaxOutputTokens"` // default: 2048

	// Cache overhead factor
	CacheOverheadFactor float64 `json:"cacheOverheadFactor"` // default: 0.05
}

func (c AdaptivePDDeciderConfig) validate() error {
	if c.InitialPrefillThroughput <= 0 {
		return errors.New("initialPrefillThroughput must be positive")
	}
	if c.InitialDecodeThroughput <= 0 {
		return errors.New("initialDecodeThroughput must be positive")
	}
	if c.KVTransferLatencyPerToken < 0 {
		return errors.New("kvTransferLatencyPerToken cannot be negative")
	}
	if c.SmallRequestThreshold < 0 || c.MediumRequestThreshold < 0 || c.LargeRequestThreshold < 0 {
		return errors.New("request thresholds cannot be negative")
	}
	if c.SmallRequestThreshold >= c.MediumRequestThreshold ||
		c.MediumRequestThreshold >= c.LargeRequestThreshold {
		return errors.New("request thresholds must be in ascending order")
	}
	if c.ThroughputWindowSize <= 0 {
		return errors.New("throughputWindowSize must be positive")
	}
	if c.CostMargin < 0 || c.CostMargin > 1 {
		return errors.New("costMargin must be between 0 and 1")
	}
	if c.DefaultMaxOutputTokens <= 0 {
		return errors.New("defaultMaxOutputTokens must be positive")
	}
	if c.CacheOverheadFactor < 0 {
		return errors.New("cacheOverheadFactor cannot be negative")
	}
	return nil
}

// AdaptiveState tracks the plugin's runtime state
type AdaptiveState struct {
	// Throughput estimates per workload class (tokens/sec)
	prefillThroughput map[WorkloadClass]float64
	decodeThroughput  map[WorkloadClass]float64

	// Thread safety
	mutex sync.RWMutex
}

func newAdaptiveState(config AdaptivePDDeciderConfig) *AdaptiveState {
	state := &AdaptiveState{
		prefillThroughput: make(map[WorkloadClass]float64),
		decodeThroughput:  make(map[WorkloadClass]float64),
	}

	// Initialize all workload classes with configured default throughput
	for _, class := range []WorkloadClass{SmallWorkload, MediumWorkload, LargeWorkload, UltraWorkload} {
		state.prefillThroughput[class] = config.InitialPrefillThroughput
		state.decodeThroughput[class] = config.InitialDecodeThroughput
	}

	return state
}

// compile-time type assertion
var _ pdDeciderPlugin = &AdaptivePDDecider{}

// AdaptivePDDecider implements a workload-aware cost-based PD disaggregation decider
type AdaptivePDDecider struct {
	typedName plugin.TypedName
	config    AdaptivePDDeciderConfig
	state     *AdaptiveState
	handle    plugin.Handle
}

// AdaptivePDDeciderPluginFactory defines the factory function for creating
// a new instance of the adaptive PD decider.
func AdaptivePDDeciderPluginFactory(name string, rawParameters json.RawMessage,
	handle plugin.Handle) (plugin.Plugin, error) {
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  defaultInitialPrefillThroughput,
		InitialDecodeThroughput:   defaultInitialDecodeThroughput,
		KVTransferLatencyPerToken: defaultKVTransferLatencyPerToken,
		SmallRequestThreshold:     defaultSmallRequestThreshold,
		MediumRequestThreshold:    defaultMediumRequestThreshold,
		LargeRequestThreshold:     defaultLargeRequestThreshold,
		ThroughputWindowSize:      defaultThroughputWindowSize,
		CostMargin:                defaultCostMargin,
		DefaultMaxOutputTokens:    defaultMaxOutputTokens,
		CacheOverheadFactor:       defaultCacheOverheadFactor,
	}

	if rawParameters != nil {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", AdaptivePDDeciderPluginType, err)
		}
	}

	decider, err := NewAdaptivePDDecider(config, handle)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s plugin: %w", AdaptivePDDeciderPluginType, err)
	}

	return decider.WithName(name), nil
}

// NewAdaptivePDDecider initializes a new adaptive PD decider plugin and returns its pointer.
// If the configuration is invalid an error is returned.
func NewAdaptivePDDecider(config AdaptivePDDeciderConfig, handle plugin.Handle) (*AdaptivePDDecider, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &AdaptivePDDecider{
		config: config,
		state:  newAdaptiveState(config),
		handle: handle,
	}, nil
}

// TypedName returns the typed name of the plugin.
func (d *AdaptivePDDecider) TypedName() plugin.TypedName {
	return d.typedName
}

// WithName sets the name of the plugin.
func (d *AdaptivePDDecider) WithName(name string) *AdaptivePDDecider {
	d.typedName.Name = name
	d.typedName.Type = AdaptivePDDeciderPluginType
	return d
}

// classifyWorkload determines the workload class based on total token count
func (d *AdaptivePDDecider) classifyWorkload(totalTokens int) WorkloadClass {
	if totalTokens < d.config.SmallRequestThreshold {
		return SmallWorkload
	} else if totalTokens < d.config.MediumRequestThreshold {
		return MediumWorkload
	} else if totalTokens < d.config.LargeRequestThreshold {
		return LargeWorkload
	}
	return UltraWorkload
}

// getPrefillWorkerState aggregates queue state across all prefill workers
func (d *AdaptivePDDecider) getPrefillWorkerState(ctx context.Context) (queueSize int, runningRequests int) {
	logger := log.FromContext(ctx).V(logutil.DEBUG)

	if d.handle == nil {
		logger.Info("Handle is nil, cannot query prefill worker state")
		return 0, 0
	}

	// Get all pods from handle
	pods := d.handle.PodList()
	if len(pods) == 0 {
		logger.Info("No pods available in handle")
		return 0, 0
	}

	// Filter for prefill pods by checking labels
	// Note: This is a simplified approach. In a real implementation, we would need
	// access to pod metadata/labels to filter properly. For now, we assume all pods
	// in the list could be prefill workers.
	// TODO: Enhance this with proper label filtering once pod metadata access is available

	totalQueue := 0
	totalRunning := 0

	// For now, we cannot filter by labels as PodList() only returns NamespacedName
	// This is a limitation of the current API. In production, this would need enhancement.
	logger.Info("Note: Unable to filter prefill pods by label with current API - using simplified estimation")

	return totalQueue, totalRunning
}

// estimateCacheOverhead estimates the overhead of cache operations
func (d *AdaptivePDDecider) estimateCacheOverhead(endpoint scheduling.Endpoint) float64 {
	if endpoint == nil {
		return 0
	}

	metrics := endpoint.GetMetrics()
	if metrics == nil {
		return 0
	}

	// Higher cache usage means more overhead for cache management
	cacheUsage := metrics.KVCacheUsagePercent
	baseOverhead := float64(d.config.DefaultMaxOutputTokens) * d.config.CacheOverheadFactor

	// Scale overhead by cache pressure
	return baseOverhead * (1.0 + cacheUsage/100.0)
}

// disaggregate implements the cost-based decision logic
func (d *AdaptivePDDecider) disaggregate(ctx context.Context, inputTokens int, endpoint scheduling.Endpoint) bool {
	logger := log.FromContext(ctx)
	debugLogger := log.FromContext(ctx).V(logutil.DEBUG)

	if endpoint == nil {
		logger.Error(nil, "adaptive decider: endpoint is nil")
		return false
	}

	// Extract parameters
	promptTokens := inputTokens
	// NOTE: maxOutputTokens would ideally be extracted from request.Body, but the disaggregate
	// interface only provides inputTokens. Using conservative default as fallback.
	maxOutputTokens := d.config.DefaultMaxOutputTokens

	totalWorkload := promptTokens + maxOutputTokens
	workloadClass := d.classifyWorkload(totalWorkload)

	debugLogger.Info("Adaptive PD decider analyzing request",
		"promptTokens", promptTokens,
		"maxOutputTokens", maxOutputTokens,
		"totalWorkload", totalWorkload,
		"workloadClass", workloadClass.String())

	// Get current queue state
	decodeMetrics := endpoint.GetMetrics()
	if decodeMetrics == nil {
		logger.Error(nil, "adaptive decider: decode endpoint metrics unavailable")
		return false
	}

	decodeQueueSize := decodeMetrics.WaitingQueueSize
	decodeRunning := decodeMetrics.RunningRequestsSize

	// Get prefill worker state (aggregated across all prefill pods)
	prefillQueueSize, prefillRunning := d.getPrefillWorkerState(ctx)

	debugLogger.Info("Current queue state",
		"decodeQueueSize", decodeQueueSize,
		"decodeRunning", decodeRunning,
		"prefillQueueSize", prefillQueueSize,
		"prefillRunning", prefillRunning)

	// Get throughput estimates for this workload class
	d.state.mutex.RLock()
	prefillThroughput := d.state.prefillThroughput[workloadClass]
	decodeThroughput := d.state.decodeThroughput[workloadClass]
	d.state.mutex.RUnlock()

	debugLogger.Info("Throughput estimates",
		"prefillThroughput", prefillThroughput,
		"decodeThroughput", decodeThroughput,
		"workloadClass", workloadClass.String())

	// Estimate costs for decode-only path
	decodeQueueDelay := 0.0
	if decodeThroughput > 0 {
		decodeQueueDelay = float64(decodeQueueSize) / decodeThroughput
	}

	prefillLatencyDecodeWorker := float64(promptTokens)/decodeThroughput + decodeQueueDelay
	decodeLatency := float64(maxOutputTokens) / decodeThroughput
	cacheOverhead := d.estimateCacheOverhead(endpoint)

	costDecodeOnly := prefillLatencyDecodeWorker + decodeLatency + cacheOverhead

	// Estimate costs for disaggregated path
	prefillQueueDelay := 0.0
	if prefillThroughput > 0 && prefillQueueSize > 0 {
		prefillQueueDelay = float64(prefillQueueSize) / prefillThroughput
	}

	prefillLatencyPrefillWorker := float64(promptTokens)/prefillThroughput + prefillQueueDelay
	decodeLatencyDisagg := float64(maxOutputTokens) / decodeThroughput
	kvTransferCost := float64(promptTokens) * d.config.KVTransferLatencyPerToken

	costDisaggregated := prefillLatencyPrefillWorker + decodeLatencyDisagg + kvTransferCost

	debugLogger.Info("Cost estimation",
		"costDecodeOnly", costDecodeOnly,
		"costDisaggregated", costDisaggregated,
		"prefillLatencyDecodeWorker", prefillLatencyDecodeWorker,
		"prefillLatencyPrefillWorker", prefillLatencyPrefillWorker,
		"decodeLatency", decodeLatency,
		"kvTransferCost", kvTransferCost,
		"cacheOverhead", cacheOverhead)

	// Apply cost margin for hysteresis
	threshold := (1 - d.config.CostMargin) * costDecodeOnly

	// Make decision: choose disaggregation if cost is significantly lower
	useDisaggregation := costDisaggregated < threshold

	// Handle edge cases
	if math.IsNaN(costDecodeOnly) || math.IsNaN(costDisaggregated) ||
	   math.IsInf(costDecodeOnly, 0) || math.IsInf(costDisaggregated, 0) {
		logger.Info("Invalid cost calculation, defaulting to no disaggregation",
			"costDecodeOnly", costDecodeOnly,
			"costDisaggregated", costDisaggregated)
		return false
	}

	logger.Info("Adaptive PD decision",
		"decision", useDisaggregation,
		"costDecodeOnly", fmt.Sprintf("%.3f", costDecodeOnly),
		"costDisaggregated", fmt.Sprintf("%.3f", costDisaggregated),
		"threshold", fmt.Sprintf("%.3f", threshold),
		"workloadClass", workloadClass.String(),
		"promptTokens", promptTokens,
		"maxOutputTokens", maxOutputTokens)

	return useDisaggregation
}
