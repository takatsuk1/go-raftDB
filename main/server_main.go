package main

import (
	"fmt"
	"go-raft/client"
	"go-raft/config"
	"go-raft/net"
	"go-raft/server"
)

func main() {

	var command string
	fmt.Println("Enter command (init/change): ")
	fmt.Scan(&command)

	switch command {
	case "init":

		var filename string
		fmt.Println("Enter the name of the YAML file (e.g., nodes.yaml): ")
		fmt.Scan(&filename)
		filepath := fmt.Sprintf("../conf/%s", filename)

		names, addrs, _, _, maxraftstate, index := config.Load_config(filepath)

		// 创建网络
		nt := net.MakeNetwork()

		server.Make_servers(names, maxraftstate, nt, addrs, index)
		fmt.Printf("Servers started: %v\n", server.SvConfig)
	case "change":
		var filename string
		fmt.Println("Enter the name of the YAML file (e.g., nodes.yaml): ")
		fmt.Scan(&filename)
		var oldfilename string
		fmt.Println("Enter the old name of the YAML file (e.g., nodes.yaml): ")
		fmt.Scan(&oldfilename)

		// 拼接文件路径
		filepath := fmt.Sprintf("../conf/%s", filename)
		oldfilepath := fmt.Sprintf("../conf/%s", oldfilename)

		// 加载配置
		names, addrs, _, _, maxraftstate, index := config.Load_config(filepath)
		oldnames, oldaddrs, _, _, _, _ := config.Load_config(oldfilepath)

		// 创建网络
		nt := net.MakeNetwork()
		server.Change_servers(oldnames, oldaddrs, names, maxraftstate, nt, addrs, index)
		cl := client.Init_ClientConfig(oldnames, oldaddrs)
		ct := cl.MakeClient()
		ct.NodesManage(oldnames, oldaddrs, names, addrs)
		fmt.Printf("Servers started: %v\n", server.SvConfig)
	}

	// 启动后服务会持续运行，直到被手动停止
	select {} // Keep the server running indefinitely
}
