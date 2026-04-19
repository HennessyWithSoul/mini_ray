package common

import "time"

const (
	NodeTypeDriver    = "driver"
	NodeTypeGCS       = "gcs"
	NodeTypeWorker    = "worker"
	NodeTypeScheduler = "scheduler"
)

const (
	EstablishResponseOK    = "OK"
	EstablishResponseError = "Error"
)

const (
	SubmitTaskTimeout = 5 * time.Second
	ShakeHandTimeout  = 5 * time.Second
	SubmitTaskSuccess = "success"
	SubmitTaskFailed  = "failed"
)

// DefaultShakehandTimeout 全局 gnet Client TCP keepalive（秒）。
const DefaultShakehandTimeout = 30
