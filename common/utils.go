package common

import "sync"

func GetSyncMapLen(m *sync.Map) int {
	len := 0
	m.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return len
}
