package metrics

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// ParseAttributeString converts a semicolon-delimited list of key=value pairs
// into OpenTelemetry attribute key-values. Whitespace around keys and values is
// trimmed. Pairs without an equals sign are treated as keys with empty string
// values.
func ParseAttributeString(packed string) []attribute.KeyValue {
	packed = strings.TrimSpace(packed)
	if packed == "" {
		return nil
	}

	segments := strings.Split(packed, ";")
	attrs := make([]attribute.KeyValue, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}

		key, value, found := strings.Cut(segment, "=")
		key = strings.TrimSpace(key)
		if !found {
			value = ""
		} else {
			value = strings.TrimSpace(value)
		}

		if key == "" {
			continue
		}
		attrs = append(attrs, attribute.String(key, value))
	}

	if len(attrs) == 0 {
		return nil
	}
	return attrs
}
