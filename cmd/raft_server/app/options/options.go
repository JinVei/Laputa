package options

import (
	"Laputa/cmd/raft_server/app/config"
	"Laputa/pkg/util/app"
	"log"
	"time"

	"github.com/spf13/pflag"
)

type Options struct {
	ElectTimeout time.Duration
	Appconf      config.Config
	LogPath      string
	ListenAddr   string
}

// New creates an Options
func New(basename string) *Options {
	return &Options{
		//LeaderElection: leaderelectionconfig.New(basename),
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.ElectTimeout, "elect-timout", time.Duration(3*time.Second),
		"Election timeout for new term")

	fs.StringVar(&o.LogPath, "log-path", "./log", "WAL log path")
	fs.StringVar(&o.ListenAddr, "listen-addr", "127.0.0.1:6677", "server listen addr")
}

// Validate will check the requirements of options
func (o *Options) Validate() []error {
	return nil
}

// process --config
func (o *Options) ApplyFlags() []error {
	err := app.UnmarshalConfig(&o.Appconf)
	if err != nil {
		log.Panic(err)
	}
	if o.Appconf.LogPath == "" {
		o.Appconf.LogPath = o.LogPath
	}
	if o.Appconf.ListenAddr == "" {
		o.Appconf.ListenAddr = o.ListenAddr
	}

	if o.Appconf.ElectTimeout == 0 {
		o.Appconf.ElectTimeout = o.ElectTimeout
	}
	return nil
}
