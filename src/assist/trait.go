package assist

import (
	"strings"
)

type MapHelper map[string]string

func (m MapHelper) Get(key string) string {
	if value, ok := m[key]; ok {
		if _value := strings.TrimSpace(value); _value != "" {
			return _value
		}
	}
	return ""
}
