// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: api/kvdb/v1alpha1/kvdb.proto

package v1alpha1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// KvdbClient is the client API for Kvdb service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KvdbClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetReply, error)
	Del(ctx context.Context, in *DelRequest, opts ...grpc.CallOption) (*DelReply, error)
}

type kvdbClient struct {
	cc grpc.ClientConnInterface
}

func NewKvdbClient(cc grpc.ClientConnInterface) KvdbClient {
	return &kvdbClient{cc}
}

func (c *kvdbClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error) {
	out := new(GetReply)
	err := c.cc.Invoke(ctx, "/laputa.kvdb.v1alpha1.Kvdb/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvdbClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetReply, error) {
	out := new(SetReply)
	err := c.cc.Invoke(ctx, "/laputa.kvdb.v1alpha1.Kvdb/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvdbClient) Del(ctx context.Context, in *DelRequest, opts ...grpc.CallOption) (*DelReply, error) {
	out := new(DelReply)
	err := c.cc.Invoke(ctx, "/laputa.kvdb.v1alpha1.Kvdb/Del", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KvdbServer is the server API for Kvdb service.
// All implementations must embed UnimplementedKvdbServer
// for forward compatibility
type KvdbServer interface {
	Get(context.Context, *GetRequest) (*GetReply, error)
	Set(context.Context, *SetRequest) (*SetReply, error)
	Del(context.Context, *DelRequest) (*DelReply, error)
	mustEmbedUnimplementedKvdbServer()
}

// UnimplementedKvdbServer must be embedded to have forward compatible implementations.
type UnimplementedKvdbServer struct {
}

func (UnimplementedKvdbServer) Get(context.Context, *GetRequest) (*GetReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedKvdbServer) Set(context.Context, *SetRequest) (*SetReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Set not implemented")
}
func (UnimplementedKvdbServer) Del(context.Context, *DelRequest) (*DelReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Del not implemented")
}
func (UnimplementedKvdbServer) mustEmbedUnimplementedKvdbServer() {}

// UnsafeKvdbServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KvdbServer will
// result in compilation errors.
type UnsafeKvdbServer interface {
	mustEmbedUnimplementedKvdbServer()
}

func RegisterKvdbServer(s grpc.ServiceRegistrar, srv KvdbServer) {
	s.RegisterService(&Kvdb_ServiceDesc, srv)
}

func _Kvdb_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KvdbServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/laputa.kvdb.v1alpha1.Kvdb/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KvdbServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Kvdb_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KvdbServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/laputa.kvdb.v1alpha1.Kvdb/Set",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KvdbServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Kvdb_Del_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KvdbServer).Del(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/laputa.kvdb.v1alpha1.Kvdb/Del",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KvdbServer).Del(ctx, req.(*DelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Kvdb_ServiceDesc is the grpc.ServiceDesc for Kvdb service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Kvdb_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "laputa.kvdb.v1alpha1.Kvdb",
	HandlerType: (*KvdbServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _Kvdb_Get_Handler,
		},
		{
			MethodName: "Set",
			Handler:    _Kvdb_Set_Handler,
		},
		{
			MethodName: "Del",
			Handler:    _Kvdb_Del_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/kvdb/v1alpha1/kvdb.proto",
}
