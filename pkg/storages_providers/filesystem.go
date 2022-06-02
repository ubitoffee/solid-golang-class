package storages_providers

import (
	"context"
	"encoding/json"
	"errors"
	"event-data-pipeline/pkg/concur"
	"event-data-pipeline/pkg/fs"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"

	"event-data-pipeline/pkg/ratelimit"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// var _ pipelines.Sink = new(FilesystemClient)

func init() {
	Register("filesystem", NewFilesystemClient)
}

// Filesystem Config includes storage settings for filesystem
type FsCfg struct {
	Path string `json:"path,omitempty"`
}

type FilesystemClient struct {
	RootDir string
	file    fs.File
	count   int
	record  chan interface{}
	mu      sync.Mutex
	ticker  *time.Ticker
	done    chan bool

	workers *concur.WorkerPool

	// 소스나 프로세서로 부터 데이터를 넘겨 받는 인풋 채널.
	inCh chan interface{}

	rateLimiter *rate.Limiter
}

// Drain implements pipelines.Sink
func (f *FilesystemClient) Drain(ctx context.Context, p payloads.Payload) error {
	// 페이로드를 받아서
	logger.Debugf("sending payload to worker input channel...")
	f.inCh <- p
	logger.Debugf("payload to worker input channel...")
	return nil
}

func NewFilesystemClient(config jsonObj) StorageProvider {
	var fsc FsCfg
	// 바이트로 변환
	cfgByte, _ := json.Marshal(config)

	// 설정파일 Struct 으로 Load
	json.Unmarshal(cfgByte, &fsc)

	fc := &FilesystemClient{
		RootDir: fsc.Path,

		inCh:   make(chan interface{}),
		record: make(chan interface{}),
		done:   make(chan bool),
		ticker: time.NewTicker(300000 * time.Millisecond),
		count:  0,
		//TODO: 설정으로부터 가져올것.
		rateLimiter: ratelimit.NewRateLimiter(*&ratelimit.RateLimit{Limit: 10, Burst: 0}),
	}

	fc.workers = concur.NewWorkerPool("filesystem-workers", fc.inCh, 1, fc.Write)
	fc.workers.Start()

	return fc
}

func (f *FilesystemClient) Write(payload interface{}) (int, error) {
	if payload != nil {

		index, docID, data := payload.(payloads.Payload).Out()

		f.mu.Lock()
		// Write 메소드가 리턴 할 때 Unlock
		defer f.mu.Unlock()
		f.file = fs.NewFile(index, docID, data)
		f.count += 1

		ctx := context.Background()
		retry := 0

		for {
			startWait := time.Now()
			f.rateLimiter.Wait(ctx)
			logger.Debugf("rate limited for %f seconds", time.Since(startWait).Seconds())

			//TODO:: 리트라이 리밋 설정값으로부터 가져올것.
			limit := 1

			defer func() {
				if err := recover(); err != nil {
					logger.Println("Write to file failed:", err)
				}
			}()

			os.MkdirAll(fmt.Sprintf("%s/%s", f.RootDir, f.file.SubDir), 0775)
			err := ioutil.WriteFile(fmt.Sprintf("%s/%s/%s", f.RootDir, f.file.SubDir, f.file.Name), f.file.Data, 0775)

			// 성공적인 쓰기에 리턴.
			if err == nil {
				return len(f.file.Data), nil
			}

			retry++
			if limit >= 0 && retry >= limit {
				return 0, err
			}
			//TODO:: 딜레이 설정 값으로부터 가져올 것.
			time.Sleep(time.Duration(5) * time.Second)
		}
	}
	return 0, errors.New("payload is nil")
}
