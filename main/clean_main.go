package main

import (
	"fmt"
	"go-raft/config"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	// 获取配置的路径
	snapPath, walPath, dbPath := config.InitPath()

	// 定义要清理的目录和文件后缀
	cleanPaths := []struct {
		dir    string
		suffix string
	}{
		{walPath, ".wal"},
		{dbPath, ".db"},
		{snapPath, ".snap"},
	}

	for _, path := range cleanPaths {
		// 确保目录存在
		if err := os.MkdirAll(path.dir, 0755); err != nil {
			fmt.Printf("创建目录失败 %s: %v\n", path.dir, err)
			continue
		}

		// 读取目录下的所有文件
		files, err := os.ReadDir(path.dir)
		if err != nil {
			fmt.Printf("读取目录失败 %s: %v\n", path.dir, err)
			continue
		}

		// 遍历并删除指定后缀的文件
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), path.suffix) {
				fullPath := filepath.Join(path.dir, file.Name())
				if err := os.Remove(fullPath); err != nil {
					fmt.Printf("删除文件失败 %s: %v\n", fullPath, err)
				} else {
					fmt.Printf("成功删除文件: %s\n", fullPath)
				}
			}
		}
	}

	fmt.Println("清理完成")
}
