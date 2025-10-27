//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdint.h>
*/
import "C"

import (
	"runtime/cgo"

	"stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go/table"
)

// table_get_location возвращает местоположение (path/URL) таблицы, связанной с указанным идентификатором (handler).
//
// Аргумент:
//   - handler: идентификатор (handle) таблицы, созданный в Go через cgo.NewHandle().
//
// Возвращает:
//   - Строка с местоположением таблицы (например, "hdfs://bucket/path/to/table").
//   - Пустую строку (`""`), если handler некорректен (<= 0).
//
// Логика работы:
// 1. Проверяет валидность handler:
//   - Если `handler <= 0`, возвращает пустую строку.
//
// 2. Извлекает объект `*table.Table` из `cgo.Handle(handler)`.
// 3. Вызывает метод `Location()` у таблицы для получения её местоположения.
//
// Используется для получения информации о физическом расположении данных таблицы,
// например, для интеграции с внешними системами или логирования.
//
//export table_get_location
func table_get_location(handler C.uintptr_t) string {
	if handler <= 0 {
		return ""
	}

	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		_ = recover()
	}()

	return cgo.Handle(handler).Value().(*table.Table).Location()
}

// table_get_metadata_location возвращает местоположение файла метаданных таблицы,
// связанной с указанным идентификатором (handler).
//
// Аргумент:
//   - handler: идентификатор (handle) таблицы, созданный в Go через cgo.NewHandle().
//
// Возвращает:
//   - Строка с путём к файлу метаданных (например, "hdfs://bucket/path/to/table/metadata.json").
//   - Пустую строку (`""`), если handler некорректен (<= 0).
//
// Логика работы:
// 1. Проверяет валидность handler:
//   - Если `handler <= 0`, возвращает пустую строку.
//
// 2. Извлекает объект `*table.Table` из `cgo.Handle(handler)`.
// 3. Вызывает метод `MetadataLocation()` у таблицы для получения пути к файлу метаданных.
//
// Используется для получения информации о расположении файла метаданных,
// например, для интеграции с внешними системами или ручного управления метаданными.
//
//export table_get_metadata_location
func table_get_metadata_location(handler C.uintptr_t) string {
	if handler <= 0 {
		return ""
	}

	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		_ = recover()
	}()

	return cgo.Handle(handler).Value().(*table.Table).MetadataLocation()
}

// table_free_handler освобождает ресурсы, связанные с идентификатором (handler), созданным в Go через cgo.NewHandle().
//
// Аргумент:
//   - handler: идентификатор (handle) таблицы, созданный в Go и переданный в C-код.
//
// Логика работы:
// 1. Вызывает метод `Delete()` у `cgo.Handle(handler)`, чтобы удалить связанный объект и освободить память.
// 2. Не возвращает значения (void).
//
// Важно:
// - Эта функция **обязательна** для вызова, когда больше нет необходимости в объекте, связанном с `handler`.
// - Не проверяет валидность `handler`. Если `handler` некорректен, будет вызвана паника с сообщением "runtime/cgo: misuse of an invalid Handle".
// - Используется для предотвращения утечек памяти при работе с объектами, созданными через `cgo.NewHandle()`.
//
//export table_free_handler
func table_free_handler(handler C.uintptr_t) {
	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		_ = recover()
	}()

	cgo.Handle(handler).Delete()
}
