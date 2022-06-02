package consumers

import (
	"context"
	"event-data-pipeline/pkg/cli"
	"event-data-pipeline/pkg/config"
	"event-data-pipeline/pkg/logger"
	"os"
	"testing"
	"time"

	"github.com/alexflint/go-arg"
)

func TestRabbitMQConsumerClient_Consume(t *testing.T) {
	//TODO: 1주차 과제입니다.
	configPath := getCurDir() + "/test/consumers/rabbitmq_consumer_test.json"
	os.Setenv("EDP_ENABLE_DEBUG_LOGGING", "true")
	os.Setenv("EDP_CONFIG", configPath)
	os.Args = nil

	arg.MustParse(&cli.Args)
	logger.Setup()

	cfg := config.NewConfig()
	pipeCfgs := config.NewPipelineConfig(cfg.PipelineCfgsPath)

	ctx := context.TODO()
	stream := make(chan interface{})
	errCh := make(chan error)

	for _, cfg := range pipeCfgs {
		cfgParams := make(jsonObj)
		pipeParams := make(jsonObj)

		// 컨텍스트
		pipeParams["context"] = ctx
		pipeParams["stream"] = stream

		// 컨슈머 에러 채널
		errCh := make(chan error)
		pipeParams["errch"] = errCh

		cfgParams["pipeParams"] = pipeParams
		cfgParams["consumerCfg"] = cfg.Consumer.Config

		rabbitMQConsumer, err := CreateConsumer(cfg.Consumer.Name, cfgParams)
		if err != nil {
			t.Error(err)
		}
		err = rabbitMQConsumer.Init()
		if err != nil {
			t.Error(err)
		}
		rabbitMQConsumer.Consume(context.TODO())
	}

	for {
		select {
		case data := <-stream:
			t.Logf("data: %v", data)
			return
		case err := <-errCh:
			t.Logf("err: %v", err)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
