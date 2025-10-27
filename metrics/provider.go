package metrics

import (
	"context"
	"fmt"
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/semconv/v1.24.0"
)

// PrometheusProvider wires the metrics SDK to a Prometheus exporter and exposes
// helpers for installing the resulting meter provider and HTTP handler.
type PrometheusProvider struct {
	provider   *sdkmetric.MeterProvider
	exporter   *prometheus.Exporter
	registerer prom.Registerer
	gatherer   prom.Gatherer
}

// PrometheusOption tweaks how the Prometheus exporter and meter provider are
// constructed.
type PrometheusOption func(*promConfig)

type promConfig struct {
	registerer prom.Registerer
	gatherer   prom.Gatherer
	resources  []*resource.Resource
}

// WithPrometheusRegisterer configures the exporter to use the supplied
// Prometheus registerer.
func WithPrometheusRegisterer(reg prom.Registerer) PrometheusOption {
	return func(cfg *promConfig) {
		cfg.registerer = reg
		if cfg.gatherer == nil {
			if g, ok := reg.(prom.Gatherer); ok {
				cfg.gatherer = g
			}
		}
	}
}

// WithPrometheusGatherer sets the gatherer that backs the exported metrics
// handler. Supply this when the registerer does not implement prom.Gatherer.
func WithPrometheusGatherer(g prom.Gatherer) PrometheusOption {
	return func(cfg *promConfig) {
		cfg.gatherer = g
	}
}

// WithResource appends the provided resource to the meter provider
// configuration.
func WithResource(res *resource.Resource) PrometheusOption {
	return func(cfg *promConfig) {
		if res != nil {
			cfg.resources = append(cfg.resources, res)
		}
	}
}

// WithResourceAttributes adds attribute key-values to the meter provider
// resource configuration.
func WithResourceAttributes(attrs ...attribute.KeyValue) PrometheusOption {
	return func(cfg *promConfig) {
		if len(attrs) == 0 {
			return
		}
		cfg.resources = append(cfg.resources, resource.NewWithAttributes(semconv.SchemaURL, attrs...))
	}
}

// WithServiceName overrides the default service.name attribute on the exported
// resource.
func WithServiceName(name string) PrometheusOption {
	return func(cfg *promConfig) {
		if name == "" {
			return
		}
		cfg.resources = append(cfg.resources, resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(name)))
	}
}

// WithServiceVersion adds a service.version attribute to the exported resource.
func WithServiceVersion(version string) PrometheusOption {
	return func(cfg *promConfig) {
		if version == "" {
			return
		}
		cfg.resources = append(cfg.resources, resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceVersionKey.String(version)))
	}
}

// NewPrometheusProvider assembles a meter provider backed by the OpenTelemetry
// Prometheus exporter using the supplied configuration options.
func NewPrometheusProvider(opts ...PrometheusOption) (*PrometheusProvider, error) {
	cfg := &promConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.registerer == nil {
		reg := prom.NewRegistry()
		cfg.registerer = reg
		cfg.gatherer = reg
	}

	if cfg.gatherer == nil {
		if g, ok := cfg.registerer.(prom.Gatherer); ok {
			cfg.gatherer = g
		} else {
			return nil, fmt.Errorf("metrics: provided Prometheus registerer does not expose a gatherer; supply WithPrometheusGatherer")
		}
	}

	if cfg.gatherer == nil {
		return nil, fmt.Errorf("metrics: no Prometheus gatherer configured")
	}

	res, err := cfg.buildResource()
	if err != nil {
		return nil, err
	}

	exporter, err := prometheus.New(prometheus.WithRegisterer(cfg.registerer))
	if err != nil {
		return nil, err
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)

	return &PrometheusProvider{
		provider:   provider,
		exporter:   exporter,
		registerer: cfg.registerer,
		gatherer:   cfg.gatherer,
	}, nil
}

func (cfg *promConfig) buildResource() (*resource.Resource, error) {
	res := resource.Default()
	var err error

	base := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String("iceberg-go"))
	res, err = resource.Merge(res, base)
	if err != nil {
		return nil, err
	}

	for _, other := range cfg.resources {
		res, err = resource.Merge(res, other)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

// MeterProvider returns the configured meter provider.
func (p *PrometheusProvider) MeterProvider() metric.MeterProvider {
	return p.provider
}

// Exporter returns the underlying Prometheus exporter instance.
func (p *PrometheusProvider) Exporter() *prometheus.Exporter {
	return p.exporter
}

// Registerer exposes the Prometheus registerer used by the exporter.
func (p *PrometheusProvider) Registerer() prom.Registerer {
	return p.registerer
}

// Gatherer returns the Prometheus gatherer associated with the exporter.
func (p *PrometheusProvider) Gatherer() prom.Gatherer {
	return p.gatherer
}

// Handler constructs an http.Handler that serves the registered metrics.
func (p *PrometheusProvider) Handler() http.Handler {
	return promhttp.HandlerFor(p.gatherer, promhttp.HandlerOpts{})
}

// InstallGlobal installs the provider as the global OpenTelemetry meter
// provider so subsequent metric helpers bind to it.
func (p *PrometheusProvider) InstallGlobal() {
	InstallMeterProvider(p.provider)
}

// Shutdown flushes outstanding metric data and releases exporter resources.
func (p *PrometheusProvider) Shutdown(ctx context.Context) error {
	return p.provider.Shutdown(ctx)
}

// InstallMeterProvider replaces the global meter provider and resets cached
// instruments so subsequent calls bind to the new provider.
func InstallMeterProvider(mp metric.MeterProvider) {
	if mp == nil {
		mp = metric.NewNoopMeterProvider()
	}

	meterMu.Lock()
	defer meterMu.Unlock()

	otel.SetMeterProvider(mp)
	resetInstrumentsLocked()
}
