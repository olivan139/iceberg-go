//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
#include <stdbool.h>
#include "libiceberg_types.h"
*/
import "C"
import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"log/slog"
	"runtime/cgo"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/apache/iceberg-go/internal/telemetry/metrics"
	"go.opentelemetry.io/otel/attribute"
	ice "stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/pkg/json2boolexpr"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/pkg/scanwire"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/table"
)

// prepare_scan_plan готовит план сканирования таблицы для передачи в C-код.
//
// Аргументы:
//   - table_handle: идентификатор (handle) таблицы, созданный в Go через `catalog_load_table()`.
//   - selected_fields: массив C-строк с именами выбираемых полей.
//   - selected_fields_count: количество элементов в массиве `selected_fields`.
//   - case_sensitive: флаг чувствительности к регистру при выборке полей.
//   - row_filter: JSON-байтовый массив с выражением фильтра.
//   - row_limit: максимальное количество строк для возврата.
//   - max_concurrency: максимальная степень параллелизма.
//   - nsegs: количество сегментов для разделения сканирования.
//     Важно: не должно быть больше чем количество сегментов в кластере!
//   - opts: идентификатор (handle) свойств сканирования (`ice.Properties`). Свойства arrow,
//     в большинстве случаев пустой, нужно инициализировать через `new_property_map()`, а потом освободить через `delete_map()`
//
// Возвращает:
//   - Структуру `C.prepare_scan_plan_result`, содержащую:
//   - `error_code`: 0 — успешное выполнение; >0 — код ошибки.
//   - `message`: текстовое описание ошибки (если произошла ошибка).
//     Если `error_code == 0`, то поле `message` игнорируется. В ином случае необходимо освободить через `free_string()`.
//   - `serialized_scan`: сериализованный план сканирования в виде байтового массива.
//   - `serialized_scan_len`: длина `serialized_scan`.
//
// Логика работы:
// 1. **Проверка входных данных**:
//   - Извлекает `ice.Properties` из `opts` через `cgo.Handle`. При ошибке возвращает `error_code = 1`.
//   - Извлекает `*table.Table` из `table_handle` через `cgo.Handle`. При ошибке возвращает `error_code = 2`.
//   - Парсит `row_filter` в `BooleanExpression` через `json2boolexpr.ParseJSON`. При ошибке возвращает `error_code = 3`.
//
// 2. **Создание сканирования**:
//   - Вызывает `tbl.Scan()` с параметрами:
//   - `selected_fields`: преобразуется в Go-срез через `CSliceToGoSlice`.
//   - `row_filter`: применённый фильтр.
//   - `row_limit`: ограничение на количество строк.
//   - `case_sensitive`: флаг чувствительности к регистру.
//   - `options`: дополнительные свойства arrow.
//   - `max_concurrency`: максимальная параллельность.
//
// 3. **Преобразование и сериализация**:
//   - Преобразует скан в формат `scanwire` через `scanwire.FromIcebergScan`. При ошибке возвращает `error_code = 4`.
//   - Сериализует результат в байты через `proto.Marshal`. При ошибке возвращает `error_code = 5`.
//
// 4. **Возврат результата**:
//   - Возвращает сериализованный план сканирования (`serialized_scan`) и его длину.
//
// Примечания:
//   - `serialized_scan` необходимо освободить после использования через `free_bytes()`.
//   - Ошибки обозначены кодами:
//     `1` — неверные свойства,
//     `2` — неверный идентификатор таблицы,
//     `3` — ошибка парсинга фильтра,
//     `4` — ошибка преобразования,
//     `5` — ошибка сериализации,
//     `6` — ошибка при извлечении значения из cgo.Handle.
//
//export prepare_scan_plan
func prepare_scan_plan(
	table_handle C.uintptr_t,
	selected_fields **C.char,
	selected_fields_count C.int32_t,
	case_sensitive C.bool,
	row_filter []byte,
	row_limit C.int64_t,
	max_concurrency C.int64_t,
	nsegs C.int32_t,
	opts C.uintptr_t,
) (result C.prepare_scan_plan_result) {
	start := time.Now()
	attrs := []attribute.KeyValue{
		attribute.Int("selected_fields", int(selected_fields_count)),
		attribute.Bool("case_sensitive", bool(case_sensitive)),
		attribute.Int64("row_limit", int64(row_limit)),
		attribute.Int("segments", int(nsegs)),
		attribute.Int64("requested_max_concurrency", int64(max_concurrency)),
		attribute.Int("effective_max_concurrency", 1),
		attribute.Bool("has_row_filter", len(row_filter) > 0),
	}

	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		if r := recover(); r != nil {
			result = C.prepare_scan_plan_result{
				error_code: 6,
				message:    C.CString(fmt.Sprintf("invalid value: %v", r)),
			}
		}
		var metricErr error
		if result.error_code != 0 {
			metricErr = fmt.Errorf("prepare_scan_plan failed with code %d", result.error_code)
		}
		metrics.RecordScanPlanTransfer(time.Since(start), metricErr, attrs...)
	}()

	currentOpts, ok := cgo.Handle(opts).Value().(ice.Properties)
	if !ok {
		result = C.prepare_scan_plan_result{
			error_code: 1,
			message:    C.CString("invalid arrow options"),
		}
		return result
	}

	tbl, ok := cgo.Handle(table_handle).Value().(*table.Table)
	if !ok {
		result = C.prepare_scan_plan_result{
			error_code: 2,
			message:    C.CString("invalid table handle"),
		}
		return result
	}

	slog.Debug(
		"row filter",
		slog.Int("len", len(row_filter)),
		slog.Int("cap", cap(row_filter)),
		slog.String("value", string(row_filter)),
	)
	filter, err := json2boolexpr.ParseJSON(row_filter)
	if err != nil {
		result = C.prepare_scan_plan_result{
			error_code: 3,
			message:    C.CString(err.Error()),
		}
		return result
	}

	s := tbl.Scan(
		table.WithSelectedFields(CSliceToGoSlice(selected_fields, selected_fields_count)...),
		table.WithRowFilter(filter),
		table.WithLimit(int64(row_limit)),
		table.WithCaseSensitive(bool(case_sensitive)),
		table.WithOptions(currentOpts),
		//table.WitMaxConcurrency(int(max_concurrency)),
		table.WitMaxConcurrency(1),
	)

	scan, err := scanwire.FromIcebergScan(
		currentCtx,
		s,
		tbl.Identifier(),
		int32(nsegs),
		tbl.MetadataLocation(),
	)
	if err != nil {
		result = C.prepare_scan_plan_result{
			error_code: 4,
			message:    C.CString(err.Error()),
		}
		return result
	}

	planTasks := 0
	if plan := scan.GetPlan(); plan != nil {
		planTasks = len(plan.GetTasks())
	}

	sscan, err := (proto.MarshalOptions{Deterministic: true, UseCachedSize: false}).Marshal(scan)
	if err != nil || len(sscan) == 0 {
		result = C.prepare_scan_plan_result{
			error_code: 5,
			message:    C.CString(err.Error()),
		}
		return result
	}

	// Кодироуем в base64 как строку
	sscan_string := base64.StdEncoding.EncodeToString(sscan)

	getMD5Hash := func() []byte {
		hasher := md5.New()
		hasher.Write([]byte(sscan_string))
		return hasher.Sum(nil)
	}

	slog.Debug(
		"scan info",
		slog.String("md5_hash", fmt.Sprintf("%x", getMD5Hash())),
		slog.Int("len", len(sscan)),
		slog.Int("b64_len", len(sscan_string)),
	)

	planAttrs := append(attrs,
		attribute.Int("plan_tasks", planTasks),
		attribute.Int("serialized_bytes", len(sscan)),
		attribute.String("encoding", "base64"),
	)
	metrics.RecordScanPlanSize(int64(len(sscan_string)), planAttrs...)

	result = C.prepare_scan_plan_result{
		error_code:      0,
		serialized_scan: C.CString(sscan_string),
	}
	return result
}

// free_bytes освобождает память, выделенную в C, для байтового массива (*C.uchar).
//
// Аргумент:
//   - bytes: указатель на байтовый массив, выделенный в C через C.malloc/C.CBytes.
//
// Логика работы:
// 1. Проверяет, не является ли `bytes` нулевым указателем.
// 2. Если `bytes != nil`, вызывает `C.free(unsafe.Pointer(bytes))` для освобождения памяти.
//
// Важно:
//   - Эта функция **обязательна** для вызова, когда больше не требуется использовать байтовый массив,
//     выделенный в C. Игнорирование вызова приведёт к утечке памяти.
//   - Не предназначена для освобождения памяти, выделенной в Go.
//
//export free_bytes
func free_bytes(bytes *C.uchar) {
	if bytes != nil {
		C.free(unsafe.Pointer(bytes))
	}
}
