//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdint.h>
*/
import "C"

import (
	"runtime/cgo"

	ice "stash.sigma.sbrf.ru/ryabina/sdp-iceberg-go"
)

// new_property_map создаёт новый пустой словарь свойств (map[string]string) и возвращает его идентификатор (handle),
// который можно использовать в C-коде для последующих операций (например, добавления пар ключ-значение).
//
// Аргументы:
//   - Нет входных параметров.
//
// Возвращает:
//   - Идентификатор (handle) типа `C.uintptr_t`, связанный с новым словарём.
//     Если произошла ошибка (например, не удалось создать объект), возвращает `0`.
//
// Логика работы:
// 1. Создаёт пустой словарь `ice.Properties` через `make(...)`.
// 2. Оборачивает его в `cgo.Handle` для безопасной передачи между Go и C.
// 3. Преобразует `cgo.Handle` в `uintptr_t` для совместимости с C.
//
// Пример использования:
//   - Вызов `new_property_map()` создаёт новый handle.
//   - Передача handle в `add_map_entry(...)` позволяет добавлять пары ключ-значение.
//   - После завершения работы с handle, вызовите `delete_map(...)` для освобождения ресурсов.
//
//export new_property_map
func new_property_map() C.uintptr_t {
	return C.uintptr_t(cgo.NewHandle(make(ice.Properties)))
}

// delete_map освобождает ресурсы, связанные с идентификатором (handle), если он валиден.
//
// Аргумент:
//   - handle: идентификатор (handle) объекта, созданного в Go через cgo.NewHandle().
//
// Логика работы:
// 1. Проверяет, является ли `handle` валидным (handle != 0).
// 2. Если `handle` корректен:
//   - Вызывает `cgo.Handle(handle).Delete()` для удаления связанного объекта и освобождения памяти.
//
// 3. Если `handle == 0`, функция завершается без действий.
//
// Используется для предотвращения утечек памяти при работе с объектами, созданными через `cgo.NewHandle()`.
// Например, после завершения использования словаря (map) или другого ресурса необходимо вызвать `delete_map`.
//
//export delete_map
func delete_map(handle C.uintptr_t) {
	if handle != 0 {
		cgo.Handle(handle).Delete()
	}
}

// add_map_entry добавляет новую пару ключ-значение в словарь (map), связанный с указанным идентификатором (handle).
//
// Аргументы:
//   - handle: идентификатор (handle) словаря, созданный в Go через cgo.NewHandle().
//   - key: C-строка с ключом для добавления.
//   - value: C-строка со значением для добавления.
//
// Логика работы:
// 1. Проверяет, является ли `handle` валидным (handle != 0).
// 2. Если `handle` корректен:
//   - Извлекает из него словарь типа `ice.Properties` через `cgo.Handle`.
//   - Преобразует `key` и `value` из C-строк в Go-строки через `C.GoString(...)`.
//   - Добавляет пару `key=value` в словарь.
//
// 3. Если `handle == 0`, функция завершается без действий.
//
// Ограничения:
// - Не проверяет существование ключа в словаре. Если ключ уже существует, его значение будет перезаписано.
// - Не возвращает значения (void).
//
//export add_map_entry
func add_map_entry(handle C.uintptr_t, key *C.char, value *C.char) {
	// Обязательно перехватываем панику которая может возникнуть при извлечении значения из cgo.Handle
	defer func() {
		_ = recover()
	}()

	if handle != 0 {
		m := cgo.Handle(handle).Value().(ice.Properties)

		m[C.GoString(key)] = C.GoString(value)
	}
}
