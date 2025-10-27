//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"unsafe"

	"runtime/cgo"

	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
	"go.opentelemetry.io/otel/attribute"
)

var (
	metricsStateMu   sync.Mutex
	activePrometheus *metrics.PrometheusProvider
)

// The C header `cmd/iceberg/include/iceberg_metrics.h` provides wrappers around
// these exported functions so they can be consumed easily from the C runtime.

//export new_property_map
func new_property_map() C.uintptr_t {
	props := make(ice.Properties)
	return C.uintptr_t(cgo.NewHandle(props))
}

//export delete_map
func delete_map(handle C.uintptr_t) {
	if handle == 0 {
		return
	}
	cgo.Handle(handle).Delete()
}

//export add_map_entry
func add_map_entry(handle C.uintptr_t, key *C.char, value *C.char) {
	defer func() {
		_ = recover()
	}()

	if handle == 0 {
		return
	}

	h := cgo.Handle(handle)
	props, ok := h.Value().(ice.Properties)
	if !ok {
		return
	}

	goKey := C.GoString(key)
	goValue := C.GoString(value)
	props[goKey] = goValue
}

//export record_histogram_sample
func record_histogram_sample(name *C.char, value C.double, attrs *C.char) *C.char {
	metricName := goString(name)
	attrList := metrics.ParseAttributeString(goString(attrs))

	if err := metrics.RecordHistogramValue(context.Background(), metricName, float64(value), attrList...); err != nil {
		return goError(err)
	}
	return nil
}

//export add_counter_sample
func add_counter_sample(name *C.char, delta C.longlong, attrs *C.char) *C.char {
	metricName := goString(name)
	attrList := metrics.ParseAttributeString(goString(attrs))

	if err := metrics.AddCounterValue(context.Background(), metricName, int64(delta), attrList...); err != nil {
		return goError(err)
	}
	return nil
}

//export install_prometheus_metrics_provider
func install_prometheus_metrics_provider(handle C.uintptr_t) *C.char {
	props, err := propertiesFromHandle(handle)
	if err != nil {
		return goError(err)
	}

	opts, handlerPath, err := buildPrometheusOptions(props)
	if err != nil {
		return goError(err)
	}

	provider, err := metrics.NewPrometheusProvider(opts...)
	if err != nil {
		return goError(err)
	}

	metricsStateMu.Lock()
	defer metricsStateMu.Unlock()

	if activePrometheus != nil {
		_ = activePrometheus.Shutdown(context.Background())
	}

	provider.InstallGlobal()
	activePrometheus = provider

	if handlerPath != "" {
		http.Handle(handlerPath, provider.Handler())
	}

	return nil
}

//export shutdown_metrics_provider
func shutdown_metrics_provider() *C.char {
	metricsStateMu.Lock()
	provider := activePrometheus
	activePrometheus = nil
	metricsStateMu.Unlock()

	if provider == nil {
		metrics.InstallMeterProvider(nil)
		return nil
	}

	metrics.InstallMeterProvider(nil)
	if err := provider.Shutdown(context.Background()); err != nil {
		return goError(err)
	}

	return nil
}

//export disable_metrics
func disable_metrics() {
	metricsStateMu.Lock()
	provider := activePrometheus
	activePrometheus = nil
	metricsStateMu.Unlock()

	metrics.InstallMeterProvider(nil)
	if provider != nil {
		_ = provider.Shutdown(context.Background())
	}
}

func propertiesFromHandle(handle C.uintptr_t) (ice.Properties, error) {
	if handle == 0 {
		return nil, nil
	}

	var (
		props ice.Properties
		ok    bool
	)

	func() {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()

		h := cgo.Handle(handle)
		props, ok = h.Value().(ice.Properties)
	}()

	if !ok {
		return nil, fmt.Errorf("invalid property map handle")
	}

	return props, nil
}

func buildPrometheusOptions(props ice.Properties) ([]metrics.PrometheusOption, string, error) {
	if props == nil {
		return nil, "", nil
	}

	opts := make([]metrics.PrometheusOption, 0, 3)
	if serviceName := strings.TrimSpace(props["service.name"]); serviceName != "" {
		opts = append(opts, metrics.WithServiceName(serviceName))
	}

	if serviceVersion := strings.TrimSpace(props["service.version"]); serviceVersion != "" {
		opts = append(opts, metrics.WithServiceVersion(serviceVersion))
	}

	attrs := make([]attribute.KeyValue, 0)
	for key, value := range props {
		if !strings.HasPrefix(key, "attr.") {
			continue
		}

		attrKey := strings.TrimPrefix(key, "attr.")
		if attrKey == "" {
			continue
		}
		attrs = append(attrs, attribute.String(attrKey, value))
	}

	if len(attrs) > 0 {
		opts = append(opts, metrics.WithResourceAttributes(attrs...))
	}

	handlerPath := strings.TrimSpace(props["prometheus.handler_path"])

	return opts, handlerPath, nil
}

func goError(err error) *C.char {
	if err == nil {
		return nil
	}
	return C.CString(err.Error())
}

func goString(str *C.char) string {
	if str == nil {
		return ""
	}
	return C.GoString(str)
}

//export free_c_string
func free_c_string(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}
