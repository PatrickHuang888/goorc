package orc

import log "github.com/sirupsen/logrus"

var logger=log.New()

func SetLogLevel(level log.Level) {
	logger.SetLevel(level)
}
