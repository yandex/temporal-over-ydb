package metrics

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ydbMetrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.temporal.io/server/common/metrics"
)

const (
	defaultNamespace = "ydb_go_sdk"
	defaultSeparator = "_"
)

var (
	defaultTimerBuckets = prometheus.ExponentialBuckets(time.Millisecond.Seconds(), 1.25, 20)
)

type Config struct {
	details      trace.Details
	separator    string
	handler      metrics.Handler
	namespace    string
	timerBuckets []float64

	m          sync.Mutex
	counters   map[metricKey]*counterVec
	gauges     map[metricKey]*gaugeVec
	timers     map[metricKey]*timerVec
	histograms map[metricKey]*histogramVec
}

func MakeConfig(registry metrics.Handler, opts ...option) *Config {
	c := &Config{
		handler:      registry,
		namespace:    defaultNamespace,
		separator:    defaultSeparator,
		timerBuckets: defaultTimerBuckets,
	}
	for _, o := range opts {
		o(c)
	}
	if c.details == 0 {
		c.details = trace.DetailsAll
	}
	return c
}

func (c *Config) CounterVec(name string, labelNames ...string) ydbMetrics.CounterVec {
	counterKey := newCounterKey(c.namespace, name)
	c.m.Lock()
	defer c.m.Unlock()
	if cnt, ok := c.counters[counterKey]; ok {
		return cnt
	}
	cnt := &counterVec{
		c: c.handler.Counter(c.namespace + c.separator + name),
	}
	c.counters[counterKey] = cnt
	return cnt
}

func (c *Config) join(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return ""
	}
	return strings.Join([]string{a, b}, c.separator)
}

func (c *Config) WithSystem(subsystem string) ydbMetrics.Config {
	return &Config{
		separator:    c.separator,
		details:      c.details,
		handler:      c.handler,
		timerBuckets: c.timerBuckets,
		namespace:    c.join(c.namespace, subsystem),
		counters:     make(map[metricKey]*counterVec),
		gauges:       make(map[metricKey]*gaugeVec),
		timers:       make(map[metricKey]*timerVec),
		histograms:   make(map[metricKey]*histogramVec),
	}
}

type metricKey struct {
	Namespace string
	Subsystem string
	Name      string
	Buckets   string
}

func newCounterKey(namespace, name string) metricKey {
	return metricKey{
		Namespace: namespace,
		Name:      name,
	}
}

func newGaugeKey(namespace, name string) metricKey {
	return metricKey{
		Namespace: namespace,
		Name:      name,
	}
}

func newHistogramKey(namespace, name string, buckets []float64) metricKey {
	return metricKey{
		Namespace: namespace,
		Name:      name,
		Buckets:   fmt.Sprintf("%v", buckets),
	}
}

func newTimerKey(namespace, name string, buckets []float64) metricKey {
	return metricKey{
		Namespace: namespace,
		Name:      name,
		Buckets:   fmt.Sprintf("%v", buckets),
	}
}

type counterVec struct {
	c metrics.CounterIface
}

func maybeReplaceReservedName(key string) string {
	if key == "name" {
		return "ydb_name"
	} else {
		return key
	}
}

func (c *counterVec) With(labels map[string]string) ydbMetrics.Counter {
	tags := make([]metrics.Tag, 0, len(labels))
	for k, v := range labels {
		k = maybeReplaceReservedName(k)
		tags = append(tags, metrics.StringTag(k, v))
	}
	return &counter{c: c.c, tags: tags}
}

type counter struct {
	c    metrics.CounterIface
	tags []metrics.Tag
}

func (c counter) Inc() {
	c.c.Record(1, c.tags...)
}

type gaugeVec struct {
	g metrics.GaugeIface
}

func (c *gaugeVec) With(labels map[string]string) ydbMetrics.Gauge {
	tags := make([]metrics.Tag, 0, len(labels))
	for k, v := range labels {
		k = maybeReplaceReservedName(k)
		tags = append(tags, metrics.StringTag(k, v))
	}
	return &gauge{c: c.g, tags: tags}
}

type gauge struct {
	c        metrics.GaugeIface
	tags     []metrics.Tag
	m        sync.Mutex
	absValue float64 // current value, for Add method
}

func (g *gauge) Add(delta float64) {
	g.m.Lock()
	defer g.m.Unlock()
	g.absValue += delta
	g.c.Record(g.absValue)
}

func (g *gauge) Set(value float64) {
	g.m.Lock()
	defer g.m.Unlock()
	g.absValue = value
	g.c.Record(value)
}

type histogramVec struct {
	h metrics.HistogramIface
}

func (h *histogramVec) With(labels map[string]string) ydbMetrics.Histogram {
	tags := make([]metrics.Tag, 0, len(labels))
	for k, v := range labels {
		k = maybeReplaceReservedName(k)
		tags = append(tags, metrics.StringTag(k, v))
	}
	return &histogram{h: h.h, tags: tags}
}

type histogram struct {
	h    metrics.HistogramIface
	tags []metrics.Tag
}

func (h *histogram) Record(v float64) {
	h.h.Record(int64(v), h.tags...)
}

type timerVec struct {
	t metrics.TimerIface
}

func (t *timerVec) With(labels map[string]string) ydbMetrics.Timer {
	tags := make([]metrics.Tag, 0, len(labels))
	for k, v := range labels {
		k = maybeReplaceReservedName(k)
		tags = append(tags, metrics.StringTag(k, v))
	}
	return &timer{t: t.t, tags: tags}
}

type timer struct {
	t    metrics.TimerIface
	tags []metrics.Tag
}

func (t *timer) Record(d time.Duration) {
	t.t.Record(d, t.tags...)
}

func (c *Config) GaugeVec(name string, labelNames ...string) ydbMetrics.GaugeVec {
	gaugeKey := newGaugeKey(c.namespace, name)
	c.m.Lock()
	defer c.m.Unlock()
	if g, ok := c.gauges[gaugeKey]; ok {
		return g
	}
	g := &gaugeVec{
		g: c.handler.Gauge(c.namespace + c.separator + name),
	}
	c.gauges[gaugeKey] = g
	return g
}

func (c *Config) TimerVec(name string, labelNames ...string) ydbMetrics.TimerVec {
	timersKey := newTimerKey(c.namespace, name, c.timerBuckets)
	c.m.Lock()
	defer c.m.Unlock()
	if t, ok := c.timers[timersKey]; ok {
		return t
	}
	t := &timerVec{
		t: c.handler.Timer(c.namespace + c.separator + name),
	}
	c.timers[timersKey] = t
	return t
}

func (c *Config) HistogramVec(name string, buckets []float64, labelNames ...string) ydbMetrics.HistogramVec {
	histogramsKey := newHistogramKey(c.namespace, name, buckets)
	c.m.Lock()
	defer c.m.Unlock()
	if h, ok := c.histograms[histogramsKey]; ok {
		return h
	}
	h := &histogramVec{
		h: c.handler.Histogram(c.namespace+c.separator+name, "ms"),
	}
	c.histograms[histogramsKey] = h
	return h
}

func (c *Config) Details() trace.Details {
	return c.details
}

type option func(*Config)

func WithNamespace(namespace string) option {
	return func(c *Config) {
		c.namespace = namespace
	}
}

func WithDetails(details trace.Details) option {
	return func(c *Config) {
		c.details |= details
	}
}

func WithSeparator(separator string) option {
	return func(c *Config) {
		c.separator = separator
	}
}

func WithTimerBuckets(timerBuckets []float64) option {
	return func(c *Config) {
		c.timerBuckets = timerBuckets
	}
}
