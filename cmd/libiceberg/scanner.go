//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
#include "libiceberg_types.h"
*/
import "C"
import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"log/slog"
	"runtime/cgo"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"google.golang.org/protobuf/proto"

	ice "stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/pkg/scanwire"
)

// init_scanner инициализирует сканер данных, преобразуя сериализованный план сканирования в поток Apache Arrow.
//
// Аргументы:
//   - serializedScan: байтовый массив с сериализованным объектом `scanwire.Scan`.
//   - segId: идентификатор сегмента (C.int32_t), используемый для фильтрации задач сканирования.
//   - props: идентификатор (handle) свойств сканирования, созданный в Go через `new_property_map()`.
//   - out: указатель на структуру `ArrowArrayStream`, которая будет заполнена результатами.
//
// Возвращает:
//   - Структуру `C.init_scanner_result`:
//   - `error_code`: 0 — успешная инициализация; >0 — код ошибки.
//   - `message`: текстовое описание ошибки (если ошибка произошла).
//
// Логика работы:
// 1. **Десериализация**:
//   - Преобразует `serializedScan` в объект `scanwire.Scan` через `proto.Unmarshal`.
//   - При ошибке возвращает `error_code = 1`.
//
// 2. **Фильтрация по сегменту**:
//   - Создаёт функцию `filterBySegmentId`, которая выбирает только те задачи, у которых `SegId == segId`.
//
// 3. **Получение свойств**:
//   - Извлекает `ice.Properties` из `props` через `cgo.Handle`.
//
// 4. **Планирование сканирования**:
//   - Вызывает `scanwire.ToPreplannedScan` для создания плана сканирования с учётом фильтра и свойств.
//   - При ошибке возвращает `error_code = 2`.
//
// 5. **Преобразование в Arrow-записи**:
//   - Использует `scan.ToArrowRecordsWithPlan` для получения схемы (`schema`) и итератора (`iter`) записей.
//   - При ошибке возвращает `error_code = 3`.
//
// 6. **Экспорт в ArrowArrayStream**:
//   - Заполняет `outStream` через `cdata.ExportRecordReader`, связывая его с `RecordReader`.
//   - Возвращает `error_code = 0` при успехе.
//
// Примечания:
// - `outStream` должен быть предварительно инициализирован нулями (например, `ArrowArrayStream stream = {0};`).
// - Ответственность за освобождение ресурсов лежит на вызывающем коде (через `release` колбэк).
//
//export init_scanner
func init_scanner(serializedScan *C.char, segId C.int32_t, props C.uintptr_t, out *C.void) (result C.init_scanner_result) {
	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		if r := recover(); r != nil {
			result = C.init_scanner_result{
				error_code: 1,
				message:    C.CString(fmt.Sprintf("invalid properties: %v", r)),
			}
		}
	}()

	outStream := (*cdata.CArrowArrayStream)(unsafe.Pointer(out))

	currentProperties, ok := cgo.Handle(props).Value().(ice.Properties)
	if !ok {
		return C.init_scanner_result{
			error_code: 2,
			message:    C.CString("invalid properties"),
		}
	}

	mppExecute := currentProperties.Get("mpp_execute", "master")

	sscan := C.GoString(serializedScan)
	sscan_bytes, err := base64.StdEncoding.DecodeString(sscan)
	if err != nil {
		return C.init_scanner_result{
			error_code: 1,
			message:    C.CString((fmt.Errorf("cant't decode sscan from base64: %w", err)).Error()),
		}
	}

	getMD5Hash := func() []byte {
		hasher := md5.New()
		hasher.Write([]byte(sscan))
		return hasher.Sum(nil)
	}

	slog.Debug(
		"scan info",
		slog.String("md5_hash", fmt.Sprintf("%x", getMD5Hash())),
		slog.Int("len", len(sscan)),
		slog.String("mpp_execute", mppExecute),
	)

	w := &scanwire.Scan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: true, AllowPartial: false, RecursionLimit: 1048576}).Unmarshal(sscan_bytes, w); err != nil {
		return C.init_scanner_result{
			error_code: 3,
			message:    C.CString(err.Error()),
		}
	}

	filterBySegmentId := func(f *scanwire.FileScanTask) bool {
		/*
			#define FTEXECLOCATION_ANY 'a'
			#define FTEXECLOCATION_MASTER 'm'
			#define FTEXECLOCATION_ALL_SEGMENTS 's'
			#define FTEXECLOCATION_NOT_DEFINED 'n'
		*/
		switch mppExecute {
		case "m", "n":
			if int32(segId) == -1 {
				return true
			}
		case "a":
			return true
		case "s":
			if int32(segId) > -1 && int32(segId) == f.GetSegId() {
				return true
			}
		}

		return false
	}

	slog.Debug(
		"scan plan",
		slog.Int("size", len(w.GetPlan().GetTasks())),
		slog.String("fields", fmt.Sprintf("[%s]", strings.Join(w.GetSelectedFields(), ","))),
	)
	scan, plan, err := scanwire.ToPreplannedScan(
		currentCtx, w,
		currentProperties,
		filterBySegmentId,
	)
	if err != nil {
		return C.init_scanner_result{
			error_code: 4,
			message:    C.CString(err.Error()),
		}
	}

	schema, iter, err := scan.ToArrowRecordsWithPlan(currentCtx, plan)
	if err != nil {
		return C.init_scanner_result{
			error_code: 5,
			message:    C.CString(err.Error()),
		}
	}

	cdata.ExportRecordReader(array.ReaderFromIter(schema, iter), outStream)

	slog.Debug(
		"scan info",
		slog.Int("len", len(sscan_bytes)),
		slog.Int("b64_len", len(sscan)),
		slog.Int("plan_size", len(plan)),
	)
	return C.init_scanner_result{error_code: 0}
}
