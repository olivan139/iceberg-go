package nodeexporter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateStage(t *testing.T) {
	tests := []struct {
		name      string
		stage     string
		wantStage string
		wantErr   bool
	}{
		{name: "empty", stage: "", wantStage: ""},
		{name: "trim", stage: "  start  ", wantStage: "start"},
		{name: "dash", stage: "pre-query", wantStage: "pre-query"},
		{name: "underscore", stage: "post_query", wantStage: "post_query"},
		{name: "invalid", stage: "bad stage", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateStage(tt.stage)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateStage error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantStage {
				t.Fatalf("validateStage = %q, want %q", got, tt.wantStage)
			}
		})
	}
}

func TestFormatMetrics(t *testing.T) {
	metrics := []Metric{{
		Name:  "libiceberg_metric_total",
		Help:  "Helpful text",
		Type:  Counter,
		Value: 42,
	}}
	got := formatMetrics(metrics, "stage")
	if !strings.Contains(got, "stage=\"stage\"") {
		t.Fatalf("expected stage label in metrics output: %s", got)
	}
	if !strings.Contains(got, "# HELP libiceberg_metric_total Helpful text") {
		t.Fatalf("unexpected metrics output: %s", got)
	}
}

func TestWriteSnapshot(t *testing.T) {
	dir := t.TempDir()

	originalReader := readCPUStat
	readCPUStat = func() (cpuStat, error) {
		return cpuStat{user: 10, system: 20, iowait: 30}, nil
	}
	defer func() { readCPUStat = originalReader }()

	if err := Init(Config{Directory: dir, FilePrefix: "testcpu"}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer func() {
		_ = Shutdown()
	}()

	path, err := WriteSnapshot("start")
	if err != nil {
		t.Fatalf("WriteSnapshot failed: %v", err)
	}

	if got, want := path, filepath.Join(dir, "testcpu_start.prom"); got != want {
		t.Fatalf("WriteSnapshot path = %q, want %q", got, want)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}

	if !strings.Contains(string(data), "libiceberg_cpu_system_jiffies_total{stage=\"start\"} 20") {
		t.Fatalf("unexpected file contents: %s", string(data))
	}
}

func TestShutdownWithoutInit(t *testing.T) {
	if err := Shutdown(); err == nil {
		t.Fatalf("expected error on Shutdown without init")
	}
}

func TestRegisterCollector(t *testing.T) {
	dir := t.TempDir()

	if err := Init(Config{Directory: dir, FilePrefix: "test"}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer func() { _ = Shutdown() }()

	collector := &stubCollector{}
	if err := RegisterCollector(collector); err != nil {
		t.Fatalf("RegisterCollector failed: %v", err)
	}

	path, err := WriteSnapshot("custom")
	if err != nil {
		t.Fatalf("WriteSnapshot failed: %v", err)
	}

	if collector.calls == 0 {
		t.Fatalf("expected custom collector to be invoked")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read metrics file: %v", err)
	}
	if !strings.Contains(string(data), "libiceberg_stub_total{source=\"stub\",stage=\"custom\"} 1") {
		t.Fatalf("expected stub metric in metrics file, got: %s", string(data))
	}
}

type stubCollector struct {
	calls int
}

func (s *stubCollector) Name() string { return "stub" }

func (s *stubCollector) Collect(string) ([]Metric, error) {
	s.calls++
	return []Metric{{
		Name:  "libiceberg_stub_total",
		Help:  "Stub metric",
		Type:  Counter,
		Value: 1,
		Labels: map[string]string{
			"source": "stub",
		},
	}}, nil
}
