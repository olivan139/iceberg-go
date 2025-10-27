package metrics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type Config struct {
	Endpoint           string
	ServiceName        string
	ServiceVersion     string
	Insecure           bool
	CollectionInterval time.Duration
}

const (
	defaultEndpoint   = "http://localhost:4318"
	defaultService    = "libiceberg"
	defaultCollection = 10 * time.Second
)

var (
	initOnce sync.Once

	provider      *sdkmetric.MeterProvider
	meter         otelmetric.Meter
	initErr       error
	providerReady atomic.Bool

	hiveDuration         otelmetric.Float64Histogram
	avroDuration         otelmetric.Float64Histogram
	hdfsDuration         otelmetric.Float64Histogram
	hdfsBytesCounter     otelmetric.Int64Counter
	filterDuration       otelmetric.Float64Histogram
	filteredBytesCounter otelmetric.Int64Counter
	scanPlanDuration     otelmetric.Float64Histogram
	scanPlanBytes        otelmetric.Int64Histogram

	counters   sync.Map // map[string]otelmetric.Int64Counter
	histograms sync.Map // map[string]otelmetric.Float64Histogram
)

var (
	ErrNotInitialized = errors.New("metrics provider not initialized")
)

var (
	componentKey = attribute.Key("component")
	operationKey = attribute.Key("operation")
	statusKey    = attribute.Key("status")
)

func (c Config) withDefaults() Config {
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
	}
	if c.ServiceName == "" {
		c.ServiceName = defaultService
	}
	if c.CollectionInterval <= 0 {
		c.CollectionInterval = defaultCollection
	}
	if c.ServiceVersion == "" {
		c.ServiceVersion = ""
	}
	if c.Endpoint != "" && strings.HasPrefix(c.Endpoint, "http://") && !c.Insecure {
		c.Insecure = true
	}
	return c
}

func Init(ctx context.Context, cfg Config) error {
	initOnce.Do(func() {
		cfg = cfg.withDefaults()

		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}

		exporter, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			initErr = err
			return
		}

		res, err := sdkresource.Merge(
			sdkresource.Default(),
			sdkresource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceName(cfg.ServiceName),
				semconv.ServiceVersion(cfg.ServiceVersion),
			),
		)
		if err != nil {
			initErr = err
			return
		}

		reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.CollectionInterval))
		provider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithResource(res),
		)

		otel.SetMeterProvider(provider)
		meter = provider.Meter("github.com/apache/iceberg-go/libiceberg")

		if err := initInstruments(); err != nil {
			initErr = err
			return
		}

		providerReady.Store(true)
	})

	return initErr
}

func initInstruments() error {
	var err error

	hiveDuration, err = meter.Float64Histogram(
		"libiceberg.hive.request.duration_ms",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Time spent performing Hive metastore RPCs"),
	)
	if err != nil {
		return err
	}

	avroDuration, err = meter.Float64Histogram(
		"libiceberg.avro.metadata.fetch.duration_ms",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Time to open and read Avro manifest metadata"),
	)
	if err != nil {
		return err
	}

	hdfsDuration, err = meter.Float64Histogram(
		"libiceberg.hdfs.request.duration_ms",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Time spent accessing HDFS"),
	)
	if err != nil {
		return err
	}

	hdfsBytesCounter, err = meter.Int64Counter(
		"libiceberg.hdfs.bytes",
		instrument.WithUnit("By"),
		instrument.WithDescription("Total bytes received from HDFS"),
	)
	if err != nil {
		return err
	}

	filterDuration, err = meter.Float64Histogram(
		"libiceberg.scan.filter.duration_ms",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Time spent evaluating scan filters"),
	)
	if err != nil {
		return err
	}

	filteredBytesCounter, err = meter.Int64Counter(
		"libiceberg.scan.filtered.bytes",
		instrument.WithUnit("By"),
		instrument.WithDescription("Bytes scheduled for transfer after filtering"),
	)
	if err != nil {
		return err
	}

	scanPlanDuration, err = meter.Float64Histogram(
		"libiceberg.scan.plan.transfer.duration_ms",
		instrument.WithUnit("ms"),
		instrument.WithDescription("Time to prepare and transfer scan plans"),
	)
	if err != nil {
		return err
	}

	scanPlanBytes, err = meter.Int64Histogram(
		"libiceberg.scan.plan.transfer.bytes",
		instrument.WithUnit("By"),
		instrument.WithDescription("Serialized scan plan size"),
	)
	if err != nil {
		return err
	}

	return nil
}

func Shutdown(ctx context.Context) error {
	if provider == nil {
		return nil
	}
	providerReady.Store(false)
	return provider.Shutdown(ctx)
}

func ensureReady() error {
	if initErr != nil {
		return initErr
	}
	if !providerReady.Load() {
		return ErrNotInitialized
	}
	return nil
}

func CounterAdd(name string, value int64, attrs ...attribute.KeyValue) error {
	if err := ensureReady(); err != nil {
		return err
	}
	ctr, err := getCounter(name)
	if err != nil {
		return err
	}
	ctr.Add(context.Background(), value, otelmetric.WithAttributes(attrs...))
	return nil
}

func HistogramRecord(name string, value float64, attrs ...attribute.KeyValue) error {
	if err := ensureReady(); err != nil {
		return err
	}
	hist, err := getHistogram(name)
	if err != nil {
		return err
	}
	hist.Record(context.Background(), value, otelmetric.WithAttributes(attrs...))
	return nil
}

func getCounter(name string) (otelmetric.Int64Counter, error) {
	if ctr, ok := counters.Load(name); ok {
		return ctr.(otelmetric.Int64Counter), nil
	}
	ctr, err := meter.Int64Counter(name)
	if err != nil {
		return nil, err
	}
	actual, _ := counters.LoadOrStore(name, ctr)
	return actual.(otelmetric.Int64Counter), nil
}

func getHistogram(name string) (otelmetric.Float64Histogram, error) {
	if hist, ok := histograms.Load(name); ok {
		return hist.(otelmetric.Float64Histogram), nil
	}
	hist, err := meter.Float64Histogram(name)
	if err != nil {
		return nil, err
	}
	actual, _ := histograms.LoadOrStore(name, hist)
	return actual.(otelmetric.Float64Histogram), nil
}

func ParseAttributeString(packed string) []attribute.KeyValue {
	if packed == "" {
		return nil
	}
	parts := strings.Split(packed, ";")
	out := make([]attribute.KeyValue, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		key := strings.TrimSpace(kv[0])
		if key == "" {
			continue
		}
		val := ""
		if len(kv) == 2 {
			val = strings.TrimSpace(kv[1])
		}
		out = append(out, attribute.String(key, val))
	}
	return out
}

func RecordHiveRequest(operation string, duration time.Duration, err error, attrs ...attribute.KeyValue) {
	recordDuration(hiveDuration, duration, err, appendAttrs(
		componentKey.String("hive"),
		operationKey.String(operation),
	), attrs...)
}

func RecordAvroMetadataFetch(duration time.Duration, err error, attrs ...attribute.KeyValue) {
	recordDuration(avroDuration, duration, err, appendAttrs(
		componentKey.String("avro"),
	), attrs...)
}

func RecordHDFSRequest(operation string, duration time.Duration, err error, attrs ...attribute.KeyValue) {
	recordDuration(hdfsDuration, duration, err, appendAttrs(
		componentKey.String("hdfs"),
		operationKey.String(operation),
	), attrs...)
}

func AddHDFSBytes(value int64, attrs ...attribute.KeyValue) {
	if value <= 0 || !providerReady.Load() {
		return
	}
	attr := append([]attribute.KeyValue{
		componentKey.String("hdfs"),
		statusKey.String("ok"),
	}, attrs...)
	hdfsBytesCounter.Add(context.Background(), value, otelmetric.WithAttributes(attr...))
}

func RecordFilteringDuration(duration time.Duration, err error, attrs ...attribute.KeyValue) {
	recordDuration(filterDuration, duration, err, appendAttrs(
		componentKey.String("scan"),
		operationKey.String("filter"),
	), attrs...)
}

func AddFilteredBytes(value int64, attrs ...attribute.KeyValue) {
	if value <= 0 || !providerReady.Load() {
		return
	}
	attr := append([]attribute.KeyValue{
		componentKey.String("scan"),
	}, attrs...)
	filteredBytesCounter.Add(context.Background(), value, otelmetric.WithAttributes(attr...))
}

func RecordScanPlanTransfer(duration time.Duration, err error, attrs ...attribute.KeyValue) {
	recordDuration(scanPlanDuration, duration, err, appendAttrs(
		componentKey.String("scan_plan"),
	), attrs...)
}

func RecordScanPlanSize(value int64, attrs ...attribute.KeyValue) {
	if value <= 0 || !providerReady.Load() {
		return
	}
	attr := append([]attribute.KeyValue{
		componentKey.String("scan_plan"),
	}, attrs...)
	scanPlanBytes.Record(context.Background(), value, otelmetric.WithAttributes(attr...))
}

func recordDuration(hist otelmetric.Float64Histogram, duration time.Duration, err error, base []attribute.KeyValue, extra ...attribute.KeyValue) {
	if !providerReady.Load() || hist == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(base)+len(extra)+1)
	attrs = append(attrs, base...)
	attrs = append(attrs, extra...)
	attrs = append(attrs, statusAttr(err))
	hist.Record(context.Background(), float64(duration)/float64(time.Millisecond), otelmetric.WithAttributes(attrs...))
}

func appendAttrs(base ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(base))
	copy(out, base)
	return out
}

func statusAttr(err error) attribute.KeyValue {
	if err != nil {
		return statusKey.String("error")
	}
	return statusKey.String("ok")
}
