//go:build cgo

package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/include -I${SRCDIR}/../../../
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"unsafe"
)

var (
	re = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
)

// CSliceToGoSlice преобразует C-массив строк (*C.char) в Go-срез строк ([]string).
//
// Аргументы:
//   - ptr: указатель на массив C-строк (например, *[]*C.char).
//   - l: длина массива (C.int32_t).
//
// Возвращает:
//   - Срез строк Go ([]string), содержащий значения из C-массива.
//
// Логика работы:
// 1. Создаёт срез `slice` размером `l`.
// 2. Для каждого элемента массива:
//   - Вычисляет адрес текущей C-строки через арифметику указателей.
//   - Преобразует C-строку в Go-строку через `C.GoString(...)`.
//   - Записывает результат в `slice[i]`.
//
// 3. Возвращает готовый срез.
//
// Пример:
//   - Если `ptr` указывает на массив ["hello", "world"], то результат будет `[]string{"hello", "world"}`.
func CSliceToGoSlice(ptr **C.char, l C.int32_t) []string {
	if ptr == nil {
		return []string{}
	}

	slice := make([]string, l)
	for i := range l {
		strPtr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + uintptr(i)*unsafe.Sizeof(ptr)))
		goStr := re.ReplaceAllString(C.GoString(strPtr), "")
		slice[i] = goStr
	}
	slog.Debug("final slice", slog.String("value", fmt.Sprintf("[%s]", strings.Join(slice, ","))))
	return slice
}
