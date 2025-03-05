package utils

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"runtime"
)

func Randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func PrintMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Mallocs: %d, Frees: %d, HeapAlloc: %d KB\n",
		m.Mallocs, m.Frees, m.HeapAlloc/1024)
}
