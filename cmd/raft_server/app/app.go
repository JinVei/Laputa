package app

import (
	"Laputa/cmd/raft_server/app/options"
	"Laputa/pkg/raft/server"
	"Laputa/pkg/util/app"
	"Laputa/pkg/util/log"
	"Laputa/pkg/util/signal"
	"os"
)

const commandDesc = `raft-server`

func New(basename string) *app.App {
	opts := options.New(basename)
	application := app.NewApp("raft-server",
		basename,
		app.WithOptions(opts),
		app.WithDescription(commandDesc),
		app.WithRunFunc(run(opts)),
	)
	return application
}

func run(opts *options.Options) app.RunFunc {
	return func(basename string) error {
		srv, err := server.New(&opts.Appconf)
		if err != nil {
			return err
		}
		if err := srv.Init(); err != nil {
			return err
		}
		go func() {
			if err := srv.Run(); err != nil {
				log.Error(err, "srv.Run() return err")
			}
			os.Exit(0)
		}()
		stopCh := signal.SetupSignalHandler()
		<-stopCh
		_ = srv.Close()
		return nil
	}
}
