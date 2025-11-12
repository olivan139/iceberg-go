//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
*/
import "C"

import (
	"log/slog"

	"github.com/apache/iceberg-go/internal/telemetry/nodeexporter"
)

//export libiceberg_node_exporter_init
func libiceberg_node_exporter_init(directory, prefix *C.char) C.int {
	cfg := nodeexporter.Config{
		Directory:  cString(directory),
		FilePrefix: cString(prefix),
	}
	if err := nodeexporter.Init(cfg); err != nil {
		slog.Error("libiceberg node exporter init failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_node_exporter_shutdown
func libiceberg_node_exporter_shutdown() C.int {
	if err := nodeexporter.Shutdown(); err != nil {
		slog.Error("libiceberg node exporter shutdown failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

//export libiceberg_node_exporter_snapshot
func libiceberg_node_exporter_snapshot(stage *C.char) C.int {
	stageStr := cString(stage)
	if _, err := nodeexporter.WriteSnapshot(stageStr); err != nil {
		slog.Error("libiceberg node exporter snapshot failed", slog.String("stage", stageStr), slog.String("error", err.Error()))
		return 1
	}
	return 0
}
