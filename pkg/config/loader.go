package config

import (
	"encoding/json"
	"event-data-pipeline/pkg/cli"
	"event-data-pipeline/pkg/logger"

	"io/ioutil"
	"os"
	"path/filepath"

	jsoniter "github.com/json-iterator/go"
	"gopkg.in/yaml.v2"
)

const (
	EXT_JSON = ".json"
	EXT_YAML = ".yaml"
	EXT_YML  = ".yml"
)

// Config configuration struct
type Config struct {
	Port             int    `json:"port,omitempty"`
	Addr             string `json:"addr,omitempty"`
	Scheme           string `json:"scheme,omitempty"`
	ProductionMode   bool   `json:"production_mode,omitempty"`
	BasePath         string `json:"base_path"`
	DebugEnabled     bool   `json:"debug_enabled,omitempty"`
	PipelineCfgsPath string `json:"logger_configs_path"`
	PipelineCfgs     []PipelineCfg
}

// PipelineCfg object is composed of a Service, Credentials, Kafka Config, and list of Processors
type PipelineCfg struct {
	Consumer   *ConsumerCfg   `json:"consumer,omitempty" yaml:"consumer,omitempty"`
	Processors []ProcessorCfg `json:"processors,omitempty" yaml:"processors,omitempty"`
	Storages   []StorageCfg   `json:"storages,omitempty" yaml:"storages,omitempty"`
}

type ProcessorCfg struct {
	Name   string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Config map[string]interface{} `json:"config,omitempty" yaml:"config,omitempty"`
}

// NewConfig creates an instance of Config from command-line args and/or env vars
func NewConfig() *Config {
	cfg := &Config{
		Port:             cli.Args.Port,
		Addr:             cli.Args.Addr,
		Scheme:           cli.Args.Scheme,
		BasePath:         cli.Args.BasePath,
		DebugEnabled:     cli.Args.DebugEnabled,
		ProductionMode:   !cli.Args.DebugEnabled,
		PipelineCfgsPath: cli.Args.Config,
	}

	return cfg
}

func NewPipelineConfig(path string) []*PipelineCfg {
	var cltrsCfArr []*PipelineCfg

	if !PathExists(path) {
		return cltrsCfArr
	}

	appendConfs := func(file string, ext string) {
		body, err := ioutil.ReadFile(file)

		if err != nil {
			logger.Panicf("error in read configuration file: %v", err)
		}
		logger.Debugf("loaded configuration >>> \n %s", string(body))

		_cltrsCfArr := UnmarshalArr(ext, body)

		//Append immediatly when configs are loaded onto array
		if len(_cltrsCfArr) > 0 {
			cltrsCfArr = append(cltrsCfArr, _cltrsCfArr...)
			return
		}

		// Unmarshal single object and append
		cltrCf := Unmarshal(ext, body)
		cltrsCfArr = append(cltrsCfArr, cltrCf)

	}

	if !IsDir(path) {
		ext := filepath.Ext(path)
		appendConfs(path, ext)
	} else {
		filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			ext := filepath.Ext(path)
			if info.Mode().IsRegular() && IsSupported(ext) {
				appendConfs(path, ext)
			}
			return nil
		})
	}

	// collector  structure differs between yaml and json unmarshalling
	// call this method to normalize between the two
	return normalizeConfigStructure(cltrsCfArr)
}

// normalizeConfigStructure takes an array of collector configs that may have been
// created from either yaml or json, and using jsoniter to help normalize the
// underlying interface{}'s
func normalizeConfigStructure(cfg []*PipelineCfg) []*PipelineCfg {
	cfsBytes, err := jsoniter.MarshalIndent(cfg, "", " ")
	var cvntdConfs []*PipelineCfg
	err = json.Unmarshal(cfsBytes, &cvntdConfs)
	if err != nil {
		logger.Errorf(err.Error())
		return nil
	}
	logger.Debugf("configs in json:\n %s", string(cfsBytes))
	return cvntdConfs
}

func Unmarshal(ext string, body []byte) *PipelineCfg {
	var cltrCf *PipelineCfg
	switch ext {
	case EXT_JSON:
		_ = json.Unmarshal([]byte(body), &cltrCf)
	case EXT_YAML, EXT_YML:
		_ = yaml.Unmarshal([]byte(body), &cltrCf)
	}
	return cltrCf
}

func UnmarshalArr(ext string, body []byte) []*PipelineCfg {
	var err error
	var _cltrsCfArr []*PipelineCfg
	switch ext {
	case EXT_JSON:
		err = json.Unmarshal([]byte(body), &_cltrsCfArr)
	case EXT_YAML, EXT_YML:
		err = yaml.Unmarshal([]byte(body), &_cltrsCfArr)
	}
	if err != nil {
		logger.Errorf("error in loading configs into array:%v", err)
	}
	return _cltrsCfArr
}

func IsSupported(ext string) bool {
	exts := map[string]bool{".json": true, ".yaml": true, ".yml": true}
	if sprt, ok := exts[ext]; ok && sprt {
		return true
	}
	return false
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		logger.Errorf("%s", err)
		return false
	}
	return true
}

func IsDir(path string) bool {
	if !PathExists(path) {
		return false
	}

	fi, _ := os.Stat(path)
	return fi.IsDir()
}
