package main

import (
	"fmt"
	"go-raft/client"
	"go-raft/config"
	"go-raft/utils"
	"os"
)

func main() {
	var filename string
	fmt.Println("Enter the name of the YAML file (e.g., nodes.yaml): ")
	fmt.Scan(&filename)

	filepath := fmt.Sprintf("../conf/%s", filename)
	names, addrs, _, _, _, _ := config.Load_config(filepath)
	cl := client.Init_ClientConfig(names, addrs)
	ct := cl.MakeClient()

	for {
		var command string
		fmt.Println("Enter command (put/get/exit/refresh): ")
		fmt.Scan(&command)

		switch command {
		case "refresh":
			cl = client.Init_ClientConfig(names, addrs)
			ct = cl.MakeClient()
		case "put":
			var key, value string
			fmt.Println("cleck创建完成，内存状况")
			utils.PrintMemStats()
			fmt.Println("Enter key: ")
			fmt.Scan(&key)
			fmt.Println("Enter value: ")
			fmt.Scan(&value)
			ct.Put(key, value)
			fmt.Printf("Put key: %s, value: %s\n", key, value)

		case "get":
			var key string
			fmt.Println("Enter key: ")
			fmt.Scan(&key)
			value := ct.Get(key)
			fmt.Printf("Got key: %s, value: %s\n", key, value)

		case "exit":
			fmt.Println("Exiting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid command")
		}
	}
}
