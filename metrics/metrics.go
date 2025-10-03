package metrics

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/apache/iceberg-go"

var (
	initOnce sync.Once

	hiveRequestDuration  metric.Float64Histogram
	avroFetchDuration    metric.Float64Histogram
	hdfsAccessDuration   metric.Float64Histogram
	hdfsDataVolume       metric.Int64Counter
	filteringDuration    metric.Float64Histogram
	filteredDataVolume   metric.Int64Counter
	scanResultDuration   metric.Float64Histogram
	scanResultDataVolume metric.Int64Counter
)

func ensureMeter() {
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
	hiveRequestDuration.Record(ctx, d.Seconds(), attribute.String("operation", operation))
}

// RecordAvroFetch records how long it took to fetch an Avro metadata artifact.
func RecordAvroFetch(ctx context.Context, kind string, d time.Duration) {
	ensureMeter()
	if avroFetchDuration == nil {
		return
	}
	avroFetchDuration.Record(ctx, d.Seconds(), attribute.String("kind", kind))
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
	hdfsAccessDuration.Record(ctx, d.Seconds(), attrs...)
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
	hdfsDataVolume.Add(ctx, bytes, attrs...)
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
