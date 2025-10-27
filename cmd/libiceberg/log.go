//go:build cgo

package main

import (
	"bytes"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"unsafe"

	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/pkg/logs"
)

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
#include "libiceberg_types.h"

static logfn_t g_logger = NULL;

static void set_g_logger(logfn_t fn) {
	if (fn != NULL) {
		g_logger = fn;
	}
}

static void call_logger(const char* msg) {
	if (g_logger != NULL) {
		g_logger(msg);
	}
}
*/
import "C"

var (
	initLogOnce sync.Once
)

//export set_logger
func set_logger(fn C.logfn_t) {
	C.set_g_logger(fn)
	initLogger()
}

type CLoggerWriter struct {
	mu  sync.Mutex
	buf []byte
}

func NewCLoggerWriter() *CLoggerWriter {
	return &CLoggerWriter{}
}

func (w *CLoggerWriter) emit(b []byte) {
	if bytes.IndexByte(b, 0) >= 0 {
		b = bytes.ReplaceAll(b, []byte{0}, []byte{' '})
	}

	cs := C.CString(string(b))
	defer C.free(unsafe.Pointer(cs))
	C.call_logger(cs)
}

func (w *CLoggerWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)

	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}

		line := w.buf[:i]
		w.emit(line)
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *CLoggerWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) > 0 {
		w.emit(w.buf)
		w.buf = w.buf[:0]
	}

	return nil
}

func initLogger() {
	initLogOnce.Do(func() {
		var opts *slog.HandlerOptions

		buildInfo, biLoaded := debug.ReadBuildInfo()

		if strings.ToLower(logs.GetDebugEnabled()) == "true" || strings.ToLower(logs.GetDebugEnabled()) == "t" || logs.GetDebugEnabled() == "1" {
			opts = &slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelDebug,
			}
		} else {
			opts = &slog.HandlerOptions{
				AddSource: false,
				Level:     slog.LevelInfo,
			}
		}

		logger := slog.New(
			slog.NewTextHandler(
				NewCLoggerWriter(),
				opts,
			),
		)

		logAttrs := make([]any, 0, 2)
		logAttrs = append(logAttrs, slog.Int("pid", os.Getpid()))
		if biLoaded {
			logAttrs = append(logAttrs, slog.String("go_version", buildInfo.GoVersion))
		}

		slog.SetDefault(logger.With(
			slog.Group("module_info", logAttrs...),
		))

		slog.Info("logger initialized")
	})
}
