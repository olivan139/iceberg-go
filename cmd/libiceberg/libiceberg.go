//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
#include "libiceberg_types.h"
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"runtime/cgo"
	"unsafe"

	ice "stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/catalog"
	_ "stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/catalog/hive"
	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/pkg/utils"
)

var (
	currentCtx     context.Context = context.Background()
	currentCatalog catalog.Catalog
)

// free_string освобождает память, выделенную под C-строку (*C.char), если она не равна nil.
//
// Аргумент:
//   - s: указатель на C-строку, созданную в Go через C.CString().
//
// Логика работы:
// 1. Проверяет, является ли указатель `s` не нулевым.
// 2. Если указатель валиден, вызывает `C.free(unsafe.Pointer(s))` для освобождения памяти.
// 3. Не возвращает значения (void).
//
// Используется для предотвращения утечек памяти при работе с C-строками, созданными в Go.
// Например, после вызова `C.CString("example")` необходимо вызвать `free_string`, чтобы освободить ресурсы.
//
//export free_string
func free_string(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

/*
После получения структуры, обязательно нужно освобождать память строки сообщения об ошибке если ошибка не 0.
Пример использования:
if (err.error_code != 0) {
    printf("Error %d: %s\n", err.error_code, err.message);
    free_string(err.message); // Освобождение памяти
} else {
    process_data(); // Обработка успешного результата
}

*/

// init_catalog инициализирует экземпляр каталога в зависимости от указанного типа и свойств.
//
// Аргументы:
//   - catalogType: тип каталога (C.CatalogTypeHive, C.CatalogTypeREST, C.CatalogTypeSQL).
//   - props: идентификатор (handle) свойств каталога, созданный в Go через `new_property_map()`.
//
// Возвращает:
//   - Структуру C.catalog_api_error:
//   - error_code: 0 — успешная инициализация; >0 — код ошибки.
//   - message: текстовое описание ошибки (если ошибка произошла).
//
// Логика работы:
// 1. Проверяет, что props не равен нулю (свойства не пустые).
// 2. Определяет тип каталога (Hive/REST/SQL) на основе catalogType.
// 3. Преобразует props в объект ice.Properties через cgo.Handle.
// 4. Загружает каталог с текущим контекстом (currentCtx) и свойствами.
// 5. При успешной загрузке возвращает error_code = 0.
// 6. При ошибке возвращает соответствующий код и сообщение.
//
//export init_catalog
func init_catalog(catalogType C.int, props C.uintptr_t) (result C.catalog_api_error) {
	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		if r := recover(); r != nil {
			result = C.catalog_api_error{
				error_code: 1,
				message:    C.CString(fmt.Sprintf("invalid properties: %v", r)),
			}
		}
	}()

	if props <= 0 {
		return C.catalog_api_error{error_code: 2, message: C.CString("properties is nil")}
	}

	var cType catalog.Type = catalog.REST

	switch catalogType {
	case C.CatalogTypeHive:
		cType = catalog.Hive
	case C.CatalogTypeREST:
		cType = catalog.REST
	case C.CatalogTypeSQL:
		cType = catalog.SQL
	}

	currentProperties, ok := cgo.Handle(props).Value().(ice.Properties)
	if !ok {
		return C.catalog_api_error{
			error_code: 3,
			message:    C.CString("invalid properties"),
		}
	}

	currentProperties["type"] = string(cType)

	// Временный костыль для отладки
	if path := os.Getenv("PATH"); len(path) > 0 {
		err := os.Setenv("PATH", fmt.Sprintf("%s:/usr/bin", path))
		if err != nil {
			return C.catalog_api_error{
				error_code: 10,
				message:    C.CString(err.Error()),
			}
		}
	} else {
		err := os.Setenv("PATH", "/usr/bin")
		if err != nil {
			return C.catalog_api_error{
				error_code: 11,
				message:    C.CString(err.Error()),
			}
		}
	}

	var err error
	currentCatalog, err = catalog.Load(currentCtx, string(cType), currentProperties)
	if err != nil {
		return C.catalog_api_error{
			error_code: 4,
			message:    C.CString(err.Error()),
		}
	}

	return C.catalog_api_error{error_code: 0}
}

// catalog_type возвращает тип текущего инициализированного каталога в виде целого числа,
// совместимого с C-перечислением (C.CatalogTypeHive, C.CatalogTypeREST, C.CatalogTypeSQL).
//
// Работает следующим образом:
// 1. Проверяет, инициализирован ли каталог (`currentCatalog != nil`).
// 2. Если каталог существует, вызывает метод `CatalogType()` для получения его типа.
// 3. Возвращает соответствующее значение:
//   - `C.CatalogTypeHive` для типа `catalog.Hive`.
//   - `C.CatalogTypeREST` для типа `catalog.REST`.
//   - `C.CatalogTypeSQL` для типа `catalog.SQL`.
//
// 4. Если каталог не инициализирован, возвращает `0`.
//
// Используется для получения информации о типе каталога в C-коде.
//
//export catalog_type
func catalog_type() C.int {
	if currentCatalog != nil {
		t := currentCatalog.CatalogType()

		switch t {
		case catalog.Hive:
			return C.CatalogTypeHive
		case catalog.REST:
			return C.CatalogTypeREST
		case catalog.SQL:
			return C.CatalogTypeSQL
		}
	}

	return 0
}

// catalog_load_table загружает таблицу из каталога Iceberg и возвращает её идентификатор.
//
// Аргументы:
//   - identifier: C-строка с именем таблицы (например, "schema.table").
//   - props: идентификатор (handle) свойств, созданный в Go через `new_property_map()`.
//
// Возвращает:
//   - Структуру `C.catalog_load_table_result`:
//   - `error_code`: 0 — успешная загрузка; >0 — код ошибки.
//   - `message`: текстовое описание ошибки (если произошла ошибка).
//   - `table_handle`: идентификатор загруженной таблицы (через `cgo.NewHandle`).
//
// Логика работы:
// 1. **Перехват паники**:
//   - Используется `defer + recover()` для перехвата возможных паник при работе с `cgo.Handle`.
//   - При панике возвращается `error_code = 1` и сообщение об ошибке.
//
// 2. **Проверка входных данных**:
//   - Если `currentCatalog == nil` → `error_code = 2`.
//   - Если `props <= 0` → `error_code = 3`.
//   - Если `identifier == nil` → `error_code = 4`.
//
// 3. **Извлечение свойств**:
//   - Попытка получить `ice.Properties` из `props` через `cgo.Handle`.
//   - При ошибке возвращается `error_code = 5`.
//
// 4. **Загрузка таблицы**:
//   - Вызов `currentCatalog.LoadTable(...)` с преобразованным идентификатором и свойствами.
//   - При ошибке возвращается `error_code = 6`.
//
// 5. **Возврат результата**:
//   - Если всё успешно, возвращается `table_handle` (новый `cgo.Handle` для таблицы).
//
// Примечания:
//   - `table_handle` должен быть освобождён после использования через `table_free_handler()`.
//   - Коды ошибок:
//     `1` — внутренняя ошибка (panic),
//     `2` — каталог не инициализирован,
//     `3` — свойства равны нулю,
//     `4` — идентификатор равен null,
//     `5` — неверные свойства,
//     `6` — ошибка загрузки таблицы.
//
//export catalog_load_table
func catalog_load_table(identifier *C.char, props C.uintptr_t) (result C.catalog_load_table_result) {
	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		if r := recover(); r != nil {
			result = C.catalog_load_table_result{
				error_code: 1,
				message:    C.CString(fmt.Sprintf("invalid properties: %v", r)),
			}
		}
	}()

	if currentCatalog == nil {
		return C.catalog_load_table_result{
			error_code: 2,
			message:    C.CString("catalog not initialized"),
		}
	}

	if props <= 0 {
		return C.catalog_load_table_result{
			error_code: 3,
			message:    C.CString("properties is nil"),
		}
	}

	if identifier == nil {
		return C.catalog_load_table_result{
			error_code: 4,
			message:    C.CString("identifier is nil"),
		}
	}

	currentProperties, ok := cgo.Handle(props).Value().(ice.Properties)
	if !ok {
		return C.catalog_load_table_result{
			error_code: 5,
			message:    C.CString("invalid properties"),
		}
	}

	table, err := currentCatalog.LoadTable(
		currentCtx,
		catalog.ToIdentifier(C.GoString(identifier)),
		currentProperties,
	)
	if err != nil {
		return C.catalog_load_table_result{
			error_code: 6,
			message:    C.CString(err.Error()),
		}
	}

	return C.catalog_load_table_result{
		error_code:   0,
		table_handle: C.uintptr_t(cgo.NewHandle(table)),
	}
}

// catalog_check_table_exists проверяет существование таблицы в текущем инициализированном каталоге.
//
// Аргументы:
//   - identifier: C-строка с именем таблицы (например, "users").
//
// Возвращает:
//   - Структуру C.catalog_check_table_exists_result:
//   - error_code: 0 — успешная проверка; >0 — код ошибки.
//   - message: текстовое описание ошибки (если ошибка произошла).
//   - result: бинарный результат (1 — таблица существует, 0 — не существует).
//
// Логика работы:
// 1. Проверяет, инициализирован ли каталог (`currentCatalog != nil`):
//   - Если нет, возвращает error_code = 1 и message = "catalog not initialized".
//
// 2. Преобразует `identifier` из C-строки в Go-строку через `C.GoString(...)`.
// 3. Вызывает метод `CheckTableExists` у `currentCatalog` с:
//   - Текущим контекстом (`currentCtx`).
//   - Идентификатором таблицы.
//
// 4. При успешной проверке:
//   - Возвращает error_code = 0 и результат в виде байта (1 — существует, 0 — не существует).
//
// 5. При ошибке:
//   - Возвращает error_code = 2 и message с описанием ошибки.
//
//export catalog_check_table_exists
func catalog_check_table_exists(identifier *C.char) C.catalog_check_table_exists_result {
	if currentCatalog == nil {
		return C.catalog_check_table_exists_result{
			error_code: 1,
			message:    C.CString("catalog not initialized"),
		}
	}

	res, err := currentCatalog.CheckTableExists(
		currentCtx,
		catalog.ToIdentifier(C.GoString(identifier)),
	)
	if err != nil {
		return C.catalog_check_table_exists_result{
			error_code: 2,
			message:    C.CString(err.Error()),
		}
	}

	return C.catalog_check_table_exists_result{
		error_code: 0,
		result:     C.char(utils.Bool2byte(res)),
	}
}

func main() {}
