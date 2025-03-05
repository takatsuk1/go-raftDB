package bench

import (
	"fmt"
	"go-raft/client"
	"go-raft/config"
	"testing"
)

// 测试 Put 操作性能
func BenchmarkPut(b *testing.B) {
	names, addrs, _, _, _, _ := config.Load_config("../conf/server.yaml")
	cl := client.Init_ClientConfig(names, addrs)
	ck := cl.MakeClient()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		ck.Put(key, value)
	}
}

// 测试 Get 操作性能
func BenchmarkGet(b *testing.B) {
	names, addrs, _, _, _, _ := config.Load_config("../conf/server.yaml")
	cl := client.Init_ClientConfig(names, addrs)
	ck := cl.MakeClient()

	// 预先插入测试数据
	key := "test-key"
	value := "test-value"
	ck.Put(key, value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ck.Get(key)
	}
}

// 测试交替读写性能
func BenchmarkMixed(b *testing.B) {
	names, addrs, _, _, _, _ := config.Load_config("../conf/server.yaml")
	cl := client.Init_ClientConfig(names, addrs)
	ck := cl.MakeClient()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			key := fmt.Sprintf("mixed-key-%d", i)
			value := fmt.Sprintf("mixed-value-%d", i)
			ck.Put(key, value)
		} else {
			key := fmt.Sprintf("mixed-key-%d", i-1)
			ck.Get(key)
		}
	}
}

// 测试并发操作性能
func BenchmarkConcurrent(b *testing.B) {
	names, addrs, _, _, _, _ := config.Load_config("../conf/server.yaml")
	cl := client.Init_ClientConfig(names, addrs)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localCk := cl.MakeClient()
		counter := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent-key-%d", counter)
			if counter%2 == 0 {
				value := fmt.Sprintf("concurrent-value-%d", counter)
				localCk.Put(key, value)
			} else {
				localCk.Get(key)
			}
			counter++
		}
	})
}
