package metrics

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/apache/iceberg-go"

var (
	meterMu  sync.Mutex
	initOnce sync.Once

	hiveRequestDuration  metric.Float64Histogram
	avroFetchDuration    metric.Float64Histogram
	hdfsAccessDuration   metric.Float64Histogram
	hdfsDataVolume       metric.Int64Counter
	filteringDuration    metric.Float64Histogram
	filteredDataVolume   metric.Int64Counter
	scanResultDuration   metric.Float64Histogram
	scanResultDataVolume metric.Int64Counter

	genericInstrumentsMu sync.Mutex
	genericHistograms    map[string]metric.Float64Histogram
	genericCounters      map[string]metric.Int64Counter
)

func ensureMeter() {
	if hiveRequestDuration != nil {
		return
	}

	meterMu.Lock()
	defer meterMu.Unlock()

	if hiveRequestDuration != nil {
		return
	}

	initOnce.Do(func() {
		m := otel.GetMeterProvider().Meter(meterName)

		var err error

		hiveRequestDuration, err = m.Float64Histogram(
			"iceberg.catalog.hive.request.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time spent performing calls to the Hive metastore."),
		)
		handle(err)

		avroFetchDuration, err = m.Float64Histogram(
			"iceberg.scan.metadata.fetch.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time spent retrieving Avro-based metadata files such as manifest lists and manifests."),
		)
		handle(err)

		hdfsAccessDuration, err = m.Float64Histogram(
			"iceberg.scan.hdfs.access.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time spent opening files from HDFS-compatible storage for scanning."),
		)
		handle(err)

		hdfsDataVolume, err = m.Int64Counter(
			"iceberg.scan.hdfs.data.volume",
			metric.WithUnit("By"),
			metric.WithDescription("Total bytes requested from HDFS-compatible storage during scanning."),
		)
		handle(err)

		filteringDuration, err = m.Float64Histogram(
			"iceberg.scan.filter.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time spent applying delete files and row-level filters during scanning."),
		)
		handle(err)

		filteredDataVolume, err = m.Int64Counter(
			"iceberg.scan.filtered.data.volume",
			metric.WithUnit("By"),
			metric.WithDescription("Bytes of data that remain after filtering has been applied."),
		)
		handle(err)

		scanResultDuration, err = m.Float64Histogram(
			"iceberg.scan.result.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time spent materialising scan results for consumers."),
		)
		handle(err)

		scanResultDataVolume, err = m.Int64Counter(
			"iceberg.scan.result.data.volume",
			metric.WithUnit("By"),
			metric.WithDescription("Bytes transferred to the consumer as part of scan results."),
		)
		handle(err)
	})
}

func resetInstrumentsLocked() {
	hiveRequestDuration = nil
	avroFetchDuration = nil
	hdfsAccessDuration = nil
	hdfsDataVolume = nil
	filteringDuration = nil
	filteredDataVolume = nil
	scanResultDuration = nil
	scanResultDataVolume = nil
	initOnce = sync.Once{}

	genericInstrumentsMu.Lock()
	genericHistograms = nil
	genericCounters = nil
	genericInstrumentsMu.Unlock()
}

func handle(err error) {
	if err != nil {
		otel.Handle(err)
	}
}

// RecordHiveRequest records the duration of a Hive metastore operation.
func RecordHiveRequest(ctx context.Context, operation string, d time.Duration) {
	ensureMeter()
	if hiveRequestDuration == nil {
		return
	}
	opts := []metric.RecordOption{}
	if operation != "" {
		opts = append(opts, metric.WithAttributes(attribute.String("operation", operation)))
	}
	hiveRequestDuration.Record(ctx, d.Seconds(), opts...)
}

// RecordAvroFetch records how long it took to fetch an Avro metadata artifact.
func RecordAvroFetch(ctx context.Context, kind string, d time.Duration) {
	ensureMeter()
	if avroFetchDuration == nil {
		return
	}
	opts := []metric.RecordOption{}
	if kind != "" {
		opts = append(opts, metric.WithAttributes(attribute.String("kind", kind)))
	}
	avroFetchDuration.Record(ctx, d.Seconds(), opts...)
}

// RecordHDFSAccess records the time required to access an HDFS-backed file.
func RecordHDFSAccess(ctx context.Context, format, content string, d time.Duration) {
	ensureMeter()
	if hdfsAccessDuration == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if format != "" {
		attrs = append(attrs, attribute.String("format", format))
	}
	if content != "" {
		attrs = append(attrs, attribute.String("content", content))
	}
	if len(attrs) > 0 {
		hdfsAccessDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
	} else {
		hdfsAccessDuration.Record(ctx, d.Seconds())
	}
}

// AddHDFSVolume records the number of bytes requested from HDFS.
func AddHDFSVolume(ctx context.Context, format, content string, bytes int64) {
	ensureMeter()
	if hdfsDataVolume == nil || bytes <= 0 {
		return
	}
	attrs := []attribute.KeyValue{}
	if format != "" {
		attrs = append(attrs, attribute.String("format", format))
	}
	if content != "" {
		attrs = append(attrs, attribute.String("content", content))
	}
	if len(attrs) > 0 {
		hdfsDataVolume.Add(ctx, bytes, metric.WithAttributes(attrs...))
	} else {
		hdfsDataVolume.Add(ctx, bytes)
	}
}

// RecordFilteringTime records the duration spent applying filtering steps.
func RecordFilteringTime(ctx context.Context, d time.Duration) {
	ensureMeter()
	if filteringDuration == nil {
		return
	}
	filteringDuration.Record(ctx, d.Seconds())
}

// AddFilteredVolume records the volume of data that remains after filtering.
func AddFilteredVolume(ctx context.Context, bytes int64) {
	ensureMeter()
	if filteredDataVolume == nil || bytes <= 0 {
		return
	}
	filteredDataVolume.Add(ctx, bytes)
}

// RecordScanResult records both the duration and resulting bytes of a completed scan.
func RecordScanResult(ctx context.Context, d time.Duration, bytes int64) {
	ensureMeter()
	if scanResultDuration != nil {
		scanResultDuration.Record(ctx, d.Seconds())
	}
	if scanResultDataVolume != nil && bytes > 0 {
		scanResultDataVolume.Add(ctx, bytes)
	}
}

var (
	errEmptyInstrumentName  = errors.New("metrics: instrument name cannot be empty")
	errNegativeCounterDelta = errors.New("metrics: counter delta must be non-negative")
)

// RecordHistogramValue records the provided value against a histogram identified
// by name. Instruments are cached so repeated calls from C or Go callers do not
// rebind every time a sample is emitted.
func RecordHistogramValue(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) error {
	if name == "" {
		return errEmptyInstrumentName
	}

	hist, err := histogramByName(name)
	if err != nil {
		return err
	}

	if len(attrs) > 0 {
		hist.Record(ctx, value, metric.WithAttributes(attrs...))
	} else {
		hist.Record(ctx, value)
	}
	return nil
}

// AddCounterValue adds the supplied delta to the named counter instrument. The
// counter is initialised on first use and cached for subsequent calls.
func AddCounterValue(ctx context.Context, name string, delta int64, attrs ...attribute.KeyValue) error {
	if name == "" {
		return errEmptyInstrumentName
	}

	if delta < 0 {
		return errNegativeCounterDelta
	}

	counter, err := counterByName(name)
	if err != nil {
		return err
	}

	if len(attrs) > 0 {
		counter.Add(ctx, delta, metric.WithAttributes(attrs...))
	} else {
		counter.Add(ctx, delta)
	}
	return nil
}

func histogramByName(name string) (metric.Float64Histogram, error) {
	genericInstrumentsMu.Lock()
	defer genericInstrumentsMu.Unlock()

	if hist, ok := genericHistograms[name]; ok && hist != nil {
		return hist, nil
	}

	m := otel.GetMeterProvider().Meter(meterName)
	hist, err := m.Float64Histogram(name)
	if err != nil {
		return nil, err
	}

	if genericHistograms == nil {
		genericHistograms = make(map[string]metric.Float64Histogram)
	}
	genericHistograms[name] = hist
	return hist, nil
}

func counterByName(name string) (metric.Int64Counter, error) {
	genericInstrumentsMu.Lock()
	defer genericInstrumentsMu.Unlock()

	if counter, ok := genericCounters[name]; ok && counter != nil {
		return counter, nil
	}

	m := otel.GetMeterProvider().Meter(meterName)
	counter, err := m.Int64Counter(name)
	if err != nil {
		return nil, err
	}

	if genericCounters == nil {
		genericCounters = make(map[string]metric.Int64Counter)
	}
	genericCounters[name] = counter
	return counter, nil
}
