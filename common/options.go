package common

type Options struct {
	maxConnections int
	ReuseAddr      bool
	ReusePort      bool
	TaskChanSize   int
}

type Option func(*Options)

func WithMaxConnections(maxConnections int) Option {
	return func(o *Options) {
		o.maxConnections = maxConnections
	}
}

func WithReuseAddr(reuseAddr bool) Option {
	return func(o *Options) {
		o.ReuseAddr = reuseAddr
	}
}

func WithReusePort(reusePort bool) Option {
	return func(o *Options) {
		o.ReusePort = reusePort
	}
}

func WithTaskChanSize(size int) Option {
	return func(o *Options) {
		o.TaskChanSize = size
	}
}
