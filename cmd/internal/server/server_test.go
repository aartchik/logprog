package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "logprog/api/v1"
	"logprog/cmd/internal/log"
	"logprog/internal/auth"
	"logprog/internal/config"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,

		"produce/consume stream succeeds": testProduceConsumeStream,

		"consume past log boundary fails": testConsumePastBoundary,

		"unauthorized fails": testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()

			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(certFile, keyFile string) (*grpc.ClientConn, api.LogClient) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile:      certFile,
			KeyFile:       keyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
		})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		conn, err := grpc.Dial(
			l.Addr().String(),
			grpc.WithTransportCredentials(tlsCreds),
		)
		require.NoError(t, err)

		return conn, api.NewLogClient(conn)
	}

	rootConn, rootClient := newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	nobodyConn, nobodyClient := newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(
		cfg,
		grpc.Creds(serverCreds),
	)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(
	t *testing.T,
	client api.LogClient,
	_ api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	_ api.LogClient,
	config *Config,
) {

	req := &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	}

	produce, err := client.Produce(context.Background(), req)
	require.NoError(t, err)

	consume, err := client.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}

}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	_ api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{
				Offset: 0,
			},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)

			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testUnauthorized(
	t *testing.T,
	_ api.LogClient,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.Nil(t, produce)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	require.Nil(t, consume)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
