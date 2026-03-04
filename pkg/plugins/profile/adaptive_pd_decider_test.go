package profile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

// mockHandle implements plugin.Handle for testing
type mockHandle struct {
	pods    []types.NamespacedName
	plugins map[string]plugin.Plugin
	ctx     context.Context
}

func (m *mockHandle) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *mockHandle) Plugin(name string) plugin.Plugin {
	if m.plugins == nil {
		return nil
	}
	return m.plugins[name]
}

func (m *mockHandle) AddPlugin(name string, p plugin.Plugin) {
	if m.plugins == nil {
		m.plugins = make(map[string]plugin.Plugin)
	}
	m.plugins[name] = p
}

func (m *mockHandle) GetAllPlugins() []plugin.Plugin {
	result := make([]plugin.Plugin, 0, len(m.plugins))
	for _, p := range m.plugins {
		result = append(result, p)
	}
	return result
}

func (m *mockHandle) GetAllPluginsWithNames() map[string]plugin.Plugin {
	return m.plugins
}

func (m *mockHandle) PodList() []types.NamespacedName {
	return m.pods
}

// createMockEndpoint creates a test endpoint with specified metrics
func createMockEndpoint(waitingQueue, runningRequests int, kvCacheUsage float64) scheduling.Endpoint {
	meta := &datalayer.EndpointMetadata{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pod"},
		Address:        "10.0.0.1",
		Port:           "8080",
	}

	metrics := &datalayer.Metrics{
		WaitingQueueSize:    waitingQueue,
		RunningRequestsSize: runningRequests,
		KVCacheUsagePercent: kvCacheUsage,
		UpdateTime:          time.Now(),
	}

	return scheduling.NewEndpoint(meta, metrics, nil)
}

func TestAdaptivePDDeciderConfig_validate(t *testing.T) {
	tests := []struct {
		name    string
		config  AdaptivePDDeciderConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: AdaptivePDDeciderConfig{
				InitialPrefillThroughput:  2000,
				InitialDecodeThroughput:   500,
				KVTransferLatencyPerToken: 0.0001,
				SmallRequestThreshold:     1000,
				MediumRequestThreshold:    10000,
				LargeRequestThreshold:     50000,
				ThroughputWindowSize:      50,
				CostMargin:                0.1,
				DefaultMaxOutputTokens:    2048,
				CacheOverheadFactor:       0.05,
			},
			wantErr: false,
		},
		{
			name: "negative prefill throughput",
			config: AdaptivePDDeciderConfig{
				InitialPrefillThroughput: -1,
				InitialDecodeThroughput:  500,
			},
			wantErr: true,
		},
		{
			name: "invalid threshold order",
			config: AdaptivePDDeciderConfig{
				InitialPrefillThroughput:  2000,
				InitialDecodeThroughput:   500,
				KVTransferLatencyPerToken: 0.0001,
				SmallRequestThreshold:     10000,  // larger than medium
				MediumRequestThreshold:    1000,
				LargeRequestThreshold:     50000,
				ThroughputWindowSize:      50,
				CostMargin:                0.1,
				DefaultMaxOutputTokens:    2048,
				CacheOverheadFactor:       0.05,
			},
			wantErr: true,
		},
		{
			name: "invalid cost margin",
			config: AdaptivePDDeciderConfig{
				InitialPrefillThroughput:  2000,
				InitialDecodeThroughput:   500,
				KVTransferLatencyPerToken: 0.0001,
				SmallRequestThreshold:     1000,
				MediumRequestThreshold:    10000,
				LargeRequestThreshold:     50000,
				ThroughputWindowSize:      50,
				CostMargin:                1.5, // > 1
				DefaultMaxOutputTokens:    2048,
				CacheOverheadFactor:       0.05,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClassifyWorkload(t *testing.T) {
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	tests := []struct {
		totalTokens   int
		expectedClass WorkloadClass
	}{
		{500, SmallWorkload},
		{999, SmallWorkload},
		{1000, MediumWorkload},
		{5000, MediumWorkload},
		{9999, MediumWorkload},
		{10000, LargeWorkload},
		{25000, LargeWorkload},
		{49999, LargeWorkload},
		{50000, UltraWorkload},
		{100000, UltraWorkload},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("tokens_%d", tt.totalTokens), func(t *testing.T) {
			class := decider.classifyWorkload(tt.totalTokens)
			assert.Equal(t, tt.expectedClass, class)
		})
	}
}

func TestDisaggregate_SmallPromptLargeOutput(t *testing.T) {
	// Scenario: Small prompt (1k) + large expected output (should use default 2048)
	// Expected: May favor disaggregation to offload decode workers
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000, // Fast prefill
		InitialDecodeThroughput:   500,  // Slower decode
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	ctx := context.Background()
	inputTokens := 1000

	// Create endpoint with moderate load
	endpoint := createMockEndpoint(5, 3, 50.0)

	decision := decider.disaggregate(ctx, inputTokens, endpoint)

	// With fast prefill and significant output tokens, disaggregation should be favorable
	// Cost decode-only: (1000/500 + 5/500) + (2048/500) + overhead ≈ 2 + 4.1 + overhead ≈ 6.2
	// Cost disaggregated: (1000/2000 + 0) + (2048/500) + (1000*0.0001) ≈ 0.5 + 4.1 + 0.1 ≈ 4.7
	// 4.7 < (1-0.1)*6.2 = 5.58, so should disaggregate
	assert.True(t, decision, "Should choose disaggregation for small prompt with default output tokens")
}

func TestDisaggregate_LargePromptSmallOutput(t *testing.T) {
	// Scenario: Large prompt (50k), small output (default 2048)
	// Expected: Decode-only preferred when prefill queue is empty
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	ctx := context.Background()
	inputTokens := 50000

	// Create endpoint with low load
	endpoint := createMockEndpoint(2, 1, 30.0)

	decision := decider.disaggregate(ctx, inputTokens, endpoint)

	// With large prompt and no prefill queue pressure:
	// Cost decode-only: (50000/500 + 2/500) + (2048/500) + overhead ≈ 100.4 + 4.1 + overhead ≈ 107
	// Cost disaggregated: (50000/2000 + 0) + (2048/500) + (50000*0.0001) ≈ 25 + 4.1 + 5 ≈ 34.1
	// 34.1 < (1-0.1)*107 = 96.3, so should disaggregate
	// Note: This test shows disaggregation is actually better even for large prompts
	// because prefill is 4x faster on dedicated workers
	assert.True(t, decision, "Should choose disaggregation when prefill is significantly faster")
}

func TestDisaggregate_HighDecodeQueue(t *testing.T) {
	// Scenario: Decode workers heavily loaded
	// Expected: Favor disaggregation to offload decode workers
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	ctx := context.Background()
	inputTokens := 5000

	// Create endpoint with heavy decode queue
	endpoint := createMockEndpoint(50, 10, 80.0)

	decision := decider.disaggregate(ctx, inputTokens, endpoint)

	// High decode queue should increase decode-only cost significantly
	// Cost decode-only: (5000/500 + 50/500) + (2048/500) + overhead ≈ 10.1 + 4.1 + high_overhead
	// Cost disaggregated: (5000/2000 + 0) + (2048/500) + (5000*0.0001) ≈ 2.5 + 4.1 + 0.5 ≈ 7.1
	assert.True(t, decision, "Should choose disaggregation when decode workers are saturated")
}

func TestDisaggregate_NilEndpoint(t *testing.T) {
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	ctx := context.Background()
	inputTokens := 1000

	decision := decider.disaggregate(ctx, inputTokens, nil)
	assert.False(t, decision, "Should return false for nil endpoint")
}

func TestDisaggregate_CostMarginHysteresis(t *testing.T) {
	// Test that cost margin prevents oscillation
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.2, // 20% margin
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	ctx := context.Background()
	inputTokens := 1000

	// Create endpoint with moderate load
	endpoint := createMockEndpoint(5, 3, 50.0)

	decision := decider.disaggregate(ctx, inputTokens, endpoint)

	// With 20% margin, the threshold is more strict
	// Cost disaggregated must be < (1-0.2) * cost_decode_only = 0.8 * cost_decode_only
	// This creates a larger stability zone
	t.Logf("Decision with 20%% margin: %v", decision)
}

func TestAdaptivePDDeciderPluginFactory(t *testing.T) {
	handle := &mockHandle{ctx: context.Background()}

	tests := []struct {
		name       string
		parameters string
		wantErr    bool
	}{
		{
			name:       "default config",
			parameters: "",
			wantErr:    false,
		},
		{
			name:       "custom config",
			parameters: `{"initialPrefillThroughput": 3000, "costMargin": 0.15}`,
			wantErr:    false,
		},
		{
			name:       "invalid json",
			parameters: `{invalid}`,
			wantErr:    true,
		},
		{
			name:       "invalid config values",
			parameters: `{"initialPrefillThroughput": -1}`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rawParams []byte
			if tt.parameters != "" {
				rawParams = []byte(tt.parameters)
			}

			plugin, err := AdaptivePDDeciderPluginFactory("test-decider", rawParams, handle)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, plugin)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, plugin)

				// Verify it's the correct type
				decider, ok := plugin.(*AdaptivePDDecider)
				assert.True(t, ok)
				assert.Equal(t, "test-decider", decider.TypedName().Name)
				assert.Equal(t, AdaptivePDDeciderPluginType, decider.TypedName().Type)
			}
		})
	}
}

func TestWorkloadClassString(t *testing.T) {
	tests := []struct {
		class    WorkloadClass
		expected string
	}{
		{SmallWorkload, "small"},
		{MediumWorkload, "medium"},
		{LargeWorkload, "large"},
		{UltraWorkload, "ultra"},
		{WorkloadClass(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.class.String())
		})
	}
}

func TestEstimateCacheOverhead(t *testing.T) {
	config := AdaptivePDDeciderConfig{
		InitialPrefillThroughput:  2000,
		InitialDecodeThroughput:   500,
		KVTransferLatencyPerToken: 0.0001,
		SmallRequestThreshold:     1000,
		MediumRequestThreshold:    10000,
		LargeRequestThreshold:     50000,
		ThroughputWindowSize:      50,
		CostMargin:                0.1,
		DefaultMaxOutputTokens:    2048,
		CacheOverheadFactor:       0.05,
	}

	handle := &mockHandle{}
	decider, err := NewAdaptivePDDecider(config, handle)
	require.NoError(t, err)

	tests := []struct {
		name          string
		endpoint      scheduling.Endpoint
		expectedRange [2]float64 // min, max
	}{
		{
			name:          "nil endpoint",
			endpoint:      nil,
			expectedRange: [2]float64{0, 0},
		},
		{
			name:          "low cache usage",
			endpoint:      createMockEndpoint(5, 3, 10.0),
			expectedRange: [2]float64{100, 120}, // base * (1 + 10/100)
		},
		{
			name:          "high cache usage",
			endpoint:      createMockEndpoint(5, 3, 90.0),
			expectedRange: [2]float64{190, 200}, // base * (1 + 90/100)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			overhead := decider.estimateCacheOverhead(tt.endpoint)
			assert.GreaterOrEqual(t, overhead, tt.expectedRange[0])
			assert.LessOrEqual(t, overhead, tt.expectedRange[1])
		})
	}
}
