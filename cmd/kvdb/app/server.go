package app

import (
	kvdbproto "Laputa/api/kvdb/v1alpha1"
	"Laputa/cmd/kvdb/app/options"
	kvdb "Laputa/pkg/kvdb"
	"context"
	"net"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

type KvdbGrpcServer struct {
	kvdbproto.UnimplementedKvdbServer
	db         *kvdb.DB
	listenaddr string
	grpcSrv    *grpc.Server
	opts       *options.Options
}

func NewKvdbGrpcServer(opts *options.Options) *KvdbGrpcServer {
	s := new(KvdbGrpcServer)
	s.db = kvdb.NewDB(opts.GetKvdbOpts())
	s.listenaddr = opts.GetListenAddr()
	s.opts = opts
	return s
}

func (s *KvdbGrpcServer) Run() error {
	klog.Info("Run KVDB grpc server")

	klog.Info("Recover kvdb ...")
	if err := s.db.Recover(); err != nil {
		klog.Errorln("Recover kvdb err: ", err)
	}

	klog.Info("Recover kvdb done")
	klog.Info("DB dir: ", s.opts.GetKvdbOpts().DBDir)

	klog.Infoln("Server listen on: ", s.listenaddr)
	lis, err := net.Listen("tcp", s.listenaddr)
	if err != nil {
		klog.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	s.grpcSrv = grpcServer
	kvdbproto.RegisterKvdbServer(s.grpcSrv, s)

	if err := grpcServer.Serve(lis); err != nil {
		klog.Fatalf("failed to serve: %s", err)
	}
	return nil
}

func (s *KvdbGrpcServer) Shutdown() error {
	s.grpcSrv.GracefulStop()
	return nil
}

func (s *KvdbGrpcServer) Get(ctx context.Context, req *kvdbproto.GetRequest) (*kvdbproto.GetReply, error) {
	klog.Infof("[KVDB.Get] req.key=%s", req.GetKey())

	val, found := s.db.Get([]byte(req.GetKey()))
	return &kvdbproto.GetReply{
		Value: string(val),
		Found: found,
	}, nil
}

func (s *KvdbGrpcServer) Set(ctx context.Context, req *kvdbproto.SetRequest) (*kvdbproto.SetReply, error) {
	klog.Infof("[KVDB.Set] req.key=%s,  req.value=%s", req.GetKey(), req.GetValue())

	err := s.db.Put([]byte(req.GetKey()), []byte(req.GetValue()))
	if err != nil {
		return nil, err
	}

	return &kvdbproto.SetReply{
		Value: req.GetValue(),
	}, nil

}

func (s *KvdbGrpcServer) Del(ctx context.Context, req *kvdbproto.DelRequest) (*kvdbproto.DelReply, error) {
	klog.Infof("[KVDB.Del] req.key=%s", req.GetKey())
	err := s.db.Delete([]byte(req.GetKey()))
	if err != nil {
		return nil, err
	}
	return &kvdbproto.DelReply{
		Value: "", // TODO
	}, nil
}
