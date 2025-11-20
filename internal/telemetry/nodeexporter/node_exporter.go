package nodeexporter

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	Directory  string
	FilePrefix string
}

type exporter struct {
	dir        string
	prefix     string
	collectors []Collector
	mu         sync.Mutex
}

var (
	globalMu sync.Mutex
	instance *exporter

	ErrAlreadyInitialized = errors.New("node exporter already initialized")
	ErrNotInitialized     = errors.New("node exporter not initialized")
	ErrNilCollector       = errors.New("collector cannot be nil")
)

type MetricType string

const (
	Counter MetricType = "counter"
	Gauge   MetricType = "gauge"
)

type Metric struct {
	Name   string
	Help   string
	Type   MetricType
	Value  float64
	Labels map[string]string
}

type Collector interface {
	Name() string
	Collect(stage string) ([]Metric, error)
}

func Init(cfg Config) error {
	cfg = cfg.withDefaults()
	if cfg.Directory == "" {
		return fmt.Errorf("node exporter directory is required")
	}
	if err := os.MkdirAll(cfg.Directory, 0o755); err != nil {
		return fmt.Errorf("create node exporter directory: %w", err)
	}

	globalMu.Lock()
	defer globalMu.Unlock()

	if instance != nil {
		return ErrAlreadyInitialized
	}

	instance = &exporter{
		dir:        cfg.Directory,
		prefix:     cfg.FilePrefix,
		collectors: []Collector{newCPUCollector()},
	}
	return nil
}

func Shutdown() error {
	globalMu.Lock()
	defer globalMu.Unlock()

	if instance == nil {
		return ErrNotInitialized
	}

	instance = nil
	return nil
}

func WriteSnapshot(stage string) (string, error) {
	globalMu.Lock()
	exp := instance
	globalMu.Unlock()

	if exp == nil {
		return "", ErrNotInitialized
	}

	return exp.write(stage)
}

func RegisterCollector(c Collector) error {
	if c == nil {
		return ErrNilCollector
	}

	globalMu.Lock()
	exp := instance
	globalMu.Unlock()

	if exp == nil {
		return ErrNotInitialized
	}

	return exp.registerCollector(c)
}

func (e *exporter) write(stage string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	stage, err := validateStage(stage)
	if err != nil {
		return "", err
	}

	metrics, err := e.collect(stage)
	if err != nil {
		return "", err
	}

	labels := stage
	fileBase := e.prefix
	if stage != "" {
		fileBase = fmt.Sprintf("%s_%s", e.prefix, stage)
	}
	filePath := filepath.Join(e.dir, fileBase+".prom")

	content := formatMetrics(metrics, labels)
	if err := writeFileAtomically(e.dir, fileBase, content, filePath); err != nil {
		return "", err
	}
	return filePath, nil
}

func (e *exporter) registerCollector(c Collector) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.collectors = append(e.collectors, c)
	return nil
}

func (e *exporter) collect(stage string) ([]Metric, error) {
	var all []Metric
	for _, c := range e.collectors {
		metrics, err := c.Collect(stage)
		if err != nil {
			return nil, fmt.Errorf("collect %s metrics: %w", c.Name(), err)
		}
		all = append(all, metrics...)
	}
	return all, nil
}

func (c Config) withDefaults() Config {
	if strings.TrimSpace(c.FilePrefix) == "" {
		c.FilePrefix = "libiceberg_cpu"
	}
	c.Directory = strings.TrimSpace(c.Directory)
	c.FilePrefix = strings.TrimSpace(c.FilePrefix)
	return c
}

type cpuStat struct {
	user   uint64
	system uint64
	iowait uint64
}

type cpuCollector struct {
	read func() (cpuStat, error)
}

var readCPUStat = readCPUTimes

func newCPUCollector() Collector {
	return &cpuCollector{read: readCPUStat}
}

func (c *cpuCollector) Name() string {
	return "cpu"
}

func (c *cpuCollector) Collect(string) ([]Metric, error) {
	stat, err := c.read()
	if err != nil {
		return nil, err
	}

	return []Metric{
		{
			Name:  "libiceberg_cpu_user_jiffies_total",
			Help:  "Cumulative CPU user jiffies observed by libiceberg.",
			Type:  Counter,
			Value: float64(stat.user),
		},
		{
			Name:  "libiceberg_cpu_system_jiffies_total",
			Help:  "Cumulative CPU system jiffies observed by libiceberg.",
			Type:  Counter,
			Value: float64(stat.system),
		},
		{
			Name:  "libiceberg_cpu_iowait_jiffies_total",
			Help:  "Cumulative CPU I/O wait jiffies observed by libiceberg.",
			Type:  Counter,
			Value: float64(stat.iowait),
		},
	}, nil
}
func readCPUTimes() (cpuStat, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return cpuStat{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 6 {
				return cpuStat{}, fmt.Errorf("unexpected cpu stat line: %q", line)
			}
			user, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return cpuStat{}, fmt.Errorf("parse user field: %w", err)
			}
			system, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil {
				return cpuStat{}, fmt.Errorf("parse system field: %w", err)
			}
			iowait, err := strconv.ParseUint(fields[5], 10, 64)
			if err != nil {
				return cpuStat{}, fmt.Errorf("parse iowait field: %w", err)
			}
			return cpuStat{user: user, system: system, iowait: iowait}, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return cpuStat{}, err
	}
	return cpuStat{}, errors.New("cpu stat line not found")
}

func formatMetrics(metrics []Metric, stage string) string {
	var sb strings.Builder
	for _, metric := range metrics {
		labels := formatLabels(metric.Labels, stage)
		sb.WriteString(fmt.Sprintf("# HELP %s %s\n", metric.Name, metric.Help))
		sb.WriteString(fmt.Sprintf("# TYPE %s %s\n", metric.Name, metric.Type))
		sb.WriteString(fmt.Sprintf("%s%s %g\n", metric.Name, labels, metric.Value))
	}
	return sb.String()
}

func formatLabels(base map[string]string, stage string) string {
	labels := make(map[string]string, len(base)+1)
	for k, v := range base {
		labels[k] = v
	}
	if stage != "" {
		labels["stage"] = stage
	}
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", k, labels[k]))
	}
	return fmt.Sprintf("{%s}", strings.Join(parts, ","))
}

func validateStage(stage string) (string, error) {
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return "", nil
	}
	for _, r := range stage {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			continue
		}
		return "", fmt.Errorf("invalid stage %q: unsupported character %q", stage, string(r))
	}
	return stage, nil
}

func writeFileAtomically(dir, base, content, target string) error {
	tmp, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
