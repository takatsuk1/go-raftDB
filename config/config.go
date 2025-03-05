package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type Server struct {
	Name    string `yaml:"name"`
	Host    string `yaml:"host"`
	IsLocal bool   `yaml:"is_local"`
}

type Config struct {
	MaxRaftState int               `yaml:"maxraftstate"`
	SnapDir      string            `yaml:"snapDir"`
	WalDir       string            `yaml:"walDir"`
	DbDir        string            `yaml:"dbDir"`
	Servers      map[string]Server `yaml:"servers"`
}

type Node struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

type NodesConfig struct {
	Nodes []Node `yaml:"nodes"`
}

func Load_config(filename string) ([]string, map[string]string, string, string, int, int) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("error reading YAML file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("error unmarshalling YAML: %v", err)
	}

	var names []string
	hosts := map[string]string{}
	manageName := ""
	manageAddress := ""
	index := -1
	tmpIndex := 0

	for name, server := range config.Servers {
		if name == "manage-server" {
			manageName = server.Name
			manageAddress = server.Host
		} else {
			names = append(names, server.Name)
			hosts[server.Name] = server.Host
			if server.IsLocal {
				index = tmpIndex
			}
			tmpIndex++
		}
	}

	return names, hosts, manageName, manageAddress, config.MaxRaftState, index
}

func InitPath() (string, string, string) {
	workDir, _ := os.Getwd()
	snapPath := filepath.Join(workDir, "snapshot")
	walPath := filepath.Join(workDir, "wal")
	dbPath := filepath.Join(workDir, "storage")

	// 读取配置文件
	configFile := filepath.Join(workDir, "conf", "server.yaml")
	if data, err := os.ReadFile(configFile); err == nil {
		var config Config
		if err := yaml.Unmarshal(data, &config); err == nil {
			if config.SnapDir != "" {
				snapPath = config.SnapDir
			}
			if config.WalDir != "" {
				walPath = config.WalDir
			}
			if config.DbDir != "" {
				dbPath = config.DbDir
			}
		}
	}

	// 确保目录存在
	os.MkdirAll(snapPath, 0755)
	os.MkdirAll(walPath, 0755)
	os.MkdirAll(dbPath, 0755)

	//fmt.Println(snapPath, walPath, dbPath)
	return snapPath, walPath, dbPath
}
