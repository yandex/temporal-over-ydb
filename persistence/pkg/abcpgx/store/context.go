package store

import "context"

type throughputPreferenceKey struct{}

// WithThroughputPreference favors throughput over latency for history batches.
// Stores may use non-transactional writes: a failed batch can partially succeed.
func WithThroughputPreference(ctx context.Context) context.Context {
	return context.WithValue(ctx, throughputPreferenceKey{}, struct{}{})
}

// PrefersThroughput reports whether the caller prefers throughput over latency.
func PrefersThroughput(ctx context.Context) bool {
	_, ok := ctx.Value(throughputPreferenceKey{}).(struct{})
	return ok
}
