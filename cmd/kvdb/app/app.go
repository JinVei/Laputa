package app

import (
	"Laputa/cmd/kvdb/app/options"
	"Laputa/pkg/util/app"
	"Laputa/pkg/util/log"
	"Laputa/pkg/util/signal"
	"os"
)

const commandDesc = `run kvdb grpc server`

func New(basename string) *app.App {
	opts := options.New()
	application := app.NewApp(
		basename,
		app.WithOptions(opts),
		app.WithDescription(commandDesc),
		app.WithRunFunc(run(opts)),
	)
	return application
}

func run(opts *options.Options) app.RunFunc {
	return func(basename string) error {
		srv := NewKvdbGrpcServer(opts)
		go func() {
			if err := srv.Run(); err != nil {
				log.Error(err, "srv.Run() return err")
			}
			os.Exit(0)
		}()
		stopCh := signal.SetupSignalHandler()
		<-stopCh
		_ = srv.Shutdown()
		return nil
	}
}
