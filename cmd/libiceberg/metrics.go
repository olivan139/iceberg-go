//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"log/slog"
	"unsafe"

	"github.com/apache/iceberg-go/internal/telemetry/metrics"
)

func cString(p *C.char) string {
	if p == nil {
		return ""
	}
	return C.GoString(p)
}

//export libiceberg_metrics_init
func libiceberg_metrics_init(endpoint, serviceName, serviceVersion *C.char) C.int {
	cfg := metrics.Config{
		Endpoint:       cString(endpoint),
		ServiceName:    cString(serviceName),
		ServiceVersion: cString(serviceVersion),
	}
	if err := metrics.Init(context.Background(), cfg); err != nil {
		slog.Error("libiceberg metrics init failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_metrics_shutdown
func libiceberg_metrics_shutdown() C.int {
	if err := metrics.Shutdown(context.Background()); err != nil {
		slog.Error("libiceberg metrics shutdown failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_metrics_histogram_record
func libiceberg_metrics_histogram_record(name *C.char, value C.double, attrs *C.char) C.int {
	attr := metrics.ParseAttributeString(cString(attrs))
	metricName := cString(name)
	if err := metrics.HistogramRecord(metricName, float64(value), attr...); err != nil {
		slog.Error("histogram record failed", slog.String("metric", metricName), slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_metrics_counter_add
func libiceberg_metrics_counter_add(name *C.char, value C.longlong, attrs *C.char) C.int {
	attr := metrics.ParseAttributeString(cString(attrs))
	metricName := cString(name)
	if err := metrics.CounterAdd(metricName, int64(value), attr...); err != nil {
		slog.Error("counter add failed", slog.String("metric", metricName), slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_metrics_free
func libiceberg_metrics_free(ptr unsafe.Pointer) {
	if ptr != nil {
		C.free(ptr)
	}
}
