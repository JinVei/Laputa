package options

import (
	"Laputa/pkg/kvdb/common"
	kvdbcomm "Laputa/pkg/kvdb/common"

	"github.com/spf13/pflag"
)

type Options struct {
	kvopts     *kvdbcomm.Options
	listenaddr string
}

func New() *Options {
	return &Options{
		kvopts: kvdbcomm.NewDefaultOptions(),
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {

	fs.StringVar(&o.kvopts.DBDir, "db-dir", o.kvopts.DBDir,
		"Kvdb base directory")
	fs.IntVar(&o.kvopts.IndexRestartInterval, "index-restart-interval", o.kvopts.IndexRestartInterval,
		"SST index block restart point interval")
	fs.IntVar(&o.kvopts.RestartInterval, "restart-interval", o.kvopts.RestartInterval,
		"SST block restart point interval")
	fs.IntVar(&o.kvopts.L0CompactTriggerNum, "l0compact-trigger", o.kvopts.L0CompactTriggerNum,
		"L0 Compact Trigger Num")
	fs.IntVar(&o.kvopts.MaxAllowSeek, "sst-max-seek", o.kvopts.MaxAllowSeek,
		"SST max allow seek")
	fs.IntVar(&o.kvopts.TableBlockSize, "sst-block-size", o.kvopts.TableBlockSize,
		"SST Block Size(bytes)")
	fs.Uint64Var(&o.kvopts.L0TableSize, "l0-table-size", o.kvopts.L0TableSize,
		"SST L0 Size(bytes)")
	fs.Uint64Var(&o.kvopts.MaxMemtableSize, "memtable-size", o.kvopts.MaxMemtableSize,
		"Memtable size(bytes)")
	fs.BoolVar(&o.kvopts.SyncWrites, "sync-writes", o.kvopts.SyncWrites,
		"Enable sync writes")

	fs.StringVar(&o.listenaddr, "listen-addr", ":8080",
		"Server listen address")
}

// Validate will check the requirements of options
func (o *Options) Validate() []error {
	return nil
}

func (o *Options) GetKvdbOpts() *common.Options {
	return o.kvopts
}

func (o *Options) GetListenAddr() string {
	return o.listenaddr
}
