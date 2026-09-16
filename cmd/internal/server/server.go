package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	api "logprog/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(
	config *Config,
	opts ...grpc.ServerOption,
) (*grpc.Server, error) {
	opts = append(opts,
		grpc.StreamInterceptor(streamAuthenticateInterceptor),
		grpc.UnaryInterceptor(unaryAuthenticateInterceptor),
	)
	gsrv := grpc.NewServer(opts...)

	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)

	return gsrv, nil
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (g *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := g.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{
		Offset: offset,
	}, nil
}

func (g *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := g.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := g.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{
		Record: record,
	}, nil
}

func (g *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := g.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

func (g *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := g.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			err = stream.Send(res)
			if err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unknown, "couldn't find peer info")
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "unexpected peer transport credentials")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "couldn't verify client certificate")
	}

	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	return context.WithValue(ctx, subjectContextKey{}, subject), nil
}

func subject(ctx context.Context) string {
	subject, _ := ctx.Value(subjectContextKey{}).(string)
	return subject
}

type subjectContextKey struct{}

func unaryAuthenticateInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	ctx, err := authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func streamAuthenticateInterceptor(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	ctx, err := authenticate(stream.Context())
	if err != nil {
		return err
	}
	return handler(srv, authenticatedServerStream{
		ServerStream: stream,
		ctx:          ctx,
	})
}

type authenticatedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s authenticatedServerStream) Context() context.Context {
	return s.ctx
}
