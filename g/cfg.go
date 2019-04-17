package g

import (
	"encoding/json"
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/toolkits/file"
)

// 保存调用Graph.GetRrd的返回值的数据结构，rrd文件内容
type File struct {
	Filename string //文件路径
	Body     []byte //文件内容
}

type HttpConfig struct {
	Enabled bool   `json:"enabled"`
	Listen  string `json:"listen"`
}

type RpcConfig struct {
	Enabled bool   `json:"enabled"`
	Listen  string `json:"listen"`
}

type RRDConfig struct {
	Storage string `json:"storage"`
}

type DBConfig struct {
	Dsn     string `json:"dsn"`
	MaxIdle int    `json:"maxIdle"`
}

type GlobalConfig struct {
	Pid         string      `json:"pid"`
	Debug       bool        `json:"debug"`
	Http        *HttpConfig `json:"http"`
	Rpc         *RpcConfig  `json:"rpc"`
	RRD         *RRDConfig  `json:"rrd"`
	DB          *DBConfig   `json:"db"`
	CallTimeout int32       `json:"callTimeout"`
	Migrate     struct {    //扩容graph时历史数据自动迁移
		Concurrency int               `json:"concurrency"` //number of multiple worker per node
		Enabled     bool              `json:"enabled"`     //表示graph是否处于数据迁移状态
		Replicas    int               `json:"replicas"`    //一致性hash副本数,必须和transfer的配置中保持一致
		Cluster     map[string]string `json:"cluster"`     //未扩容前老的graph实例列表
	} `json:"migrate"`
}

var (
	ConfigFile string
	ptr        unsafe.Pointer
)

func Config() *GlobalConfig {
	return (*GlobalConfig)(atomic.LoadPointer(&ptr))
}

func ParseConfig(cfg string) {
	if cfg == "" {
		log.Fatalln("config file not specified: use -c $filename")
	}

	if !file.IsExist(cfg) {
		log.Fatalln("config file specified not found:", cfg)
	}

	ConfigFile = cfg

	configContent, err := file.ToTrimString(cfg)
	if err != nil {
		log.Fatalln("read config file", cfg, "error:", err.Error())
	}

	var c GlobalConfig
	err = json.Unmarshal([]byte(configContent), &c)
	if err != nil {
		log.Fatalln("parse config file", cfg, "error:", err.Error())
	}

	if c.Migrate.Enabled && len(c.Migrate.Cluster) == 0 {
		c.Migrate.Enabled = false
	}

	// set config
	atomic.StorePointer(&ptr, unsafe.Pointer(&c))

	log.Println("g.ParseConfig ok, file", cfg)
}
