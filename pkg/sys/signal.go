package sys

import (
	"event-data-pipeline/pkg/logger"
	"os"
	"os/signal"
)

type Signal struct {
	shutdown chan os.Signal
	done     chan bool
}

func NewSignal(signals ...os.Signal) *Signal {
	s := &Signal{
		shutdown: make(chan os.Signal),
		done:     make(chan bool),
	}
	s.Notify(signals...)
	return s
}
func (s *Signal) Notify(signals ...os.Signal) {
	signal.Notify(s.shutdown, signals...)
}

func (s *Signal) SendShutDown(signal os.Signal) {
	logger.Infof("sending shutdown signal:%s", signal)
	s.shutdown <- signal
}

func (s *Signal) ReceiveShutDown() {
	signal := <-s.shutdown
	logger.Infof("receiving shutdown signal:%s", signal)
}

func (s *Signal) SendDone(signal bool) {
	s.done <- signal
	logger.Infof("sent done signal: %v", signal)
}

func (s *Signal) ReceiveDone() {
	signal := <-s.done
	logger.Infof("receiving done signal: %v", signal)
}
