package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"logprog/internal/auth"
	"logprog/internal/discovery"
	"logprog/internal/log"
	"logprog/internal/server"
)

type Agent struct {
	Config Config

	// Один TCP-порт разделяем между Raft и gRPC.
	mux cmux.CMux

	// Теперь используем DistributedLog с Raft,
	// а старый Replicator больше не нужен.
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config

	DataDir        string
	BindAddr       string // адрес Serf
	RPCPort        int    // общий порт для gRPC + Raft
	NodeName       string
	StartJoinAddrs []string

	ACLModelFile  string
	ACLPolicyFile string

	// Первая нода создаёт первоначальный Raft-кластер.
	Bootstrap bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	// Запускаем cmux, который начинает принимать соединения
	// и распределять их между Raft и gRPC.
	go a.serve()

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)

	return nil
}

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		a.Config.RPCPort,
	)

	// Один физический TCP listener.
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	// cmux будет делить соединения между Raft и gRPC.
	a.mux = cmux.New(ln)

	return nil
}

func (a *Agent) setupLog() error {
	// Raft-соединение первым байтом отправляет RaftRPC (= 1).
	// Поэтому cmux может отличить Raft от gRPC.
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)

		if _, err := reader.Read(b); err != nil {
			return false
		}

		return bytes.Compare(
			b,
			[]byte{byte(log.RaftRPC)},
		) == 0
	})

	logConfig := log.Config{}

	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)

	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error

	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}

	// Первая нода bootstrap'ит кластер и должна дождаться,
	// пока Raft выберет лидера.
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}

	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
		GetServerer: a.log,
	}

	var opts []grpc.ServerOption

	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(
			a.Config.ServerTLSConfig,
		)

		opts = append(
			opts,
			grpc.Creds(creds),
		)
	}

	var err error

	a.server, err = server.NewGRPCServer(
		serverConfig,
		opts...,
	)
	if err != nil {
		return err
	}

	// Всё, что cmux не распознал как Raft,
	// отдаём gRPC-серверу.
	grpcLn := a.mux.Match(cmux.Any())

	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	// ВАЖНО:
	// handler теперь a.log (DistributedLog).
	//
	// Serf обнаруживает ноду
	// -> Membership вызывает a.log.Join(...)
	// -> DistributedLog добавляет её в Raft.
	a.membership, err = discovery.New(
		a.log,
		discovery.Config{
			NodeName: a.Config.NodeName,
			BindAddr: a.Config.BindAddr,

			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},

			StartJoinAddrs: a.Config.StartJoinAddrs,
		},
	)

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	// Сначала сообщаем Serf, что нода уходит.
	if err := a.membership.Leave(); err != nil {
		return err
	}

	// Replicator.Close() здесь БОЛЬШЕ НЕТ.
	// Репликацией теперь занимается Raft.

	// Останавливаем gRPC.
	a.server.GracefulStop()

	// DistributedLog.Close() остановит Raft
	// и закроет локальный log.
	if err := a.log.Close(); err != nil {
		return err
	}

	return nil
}

func (a *Agent) serve() error {
	// Запускаем настоящий listener.
	// cmux будет распределять соединения:
	//
	// первый байт == RaftRPC -> Raft
	// всё остальное          -> gRPC
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}

	return nil
}
