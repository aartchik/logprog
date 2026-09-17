package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "logprog/api/v1"
	"logprog/internal/auth"
	"logprog/internal/discovery"
	"logprog/internal/log"

	"logprog/internal/server"
)

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config

	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string

	ACLModelFile  string
	ACLPolicyFile string
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
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

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

func (a *Agent) setupLog() error {
	var err error

	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)

	return err
}

func (a *Agent) setupServer() error {
	// Создаём систему авторизации.
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	// Наш gRPC-сервер будет работать именно с локальным Log этой ноды.
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption

	// Если настроен TLS, заставляем gRPC-сервер использовать его.
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// Получаем адрес именно gRPC-сервера.
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	// Serve блокируется, поэтому запускаем сервер в отдельной горутине.
	go func() {
		if err := a.server.Serve(ln); err != nil {
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

	var opts []grpc.DialOption

	// Эти credentials используются, когда эта нода сама выступает
	// gRPC-клиентом и подключается к другой ноде.
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts,
			grpc.WithTransportCredentials(
				credentials.NewTLS(a.Config.PeerTLSConfig),
			),
		)
	}

	// Подключаемся к собственному gRPC-серверу.
	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}

	client := api.NewLogClient(conn)

	// Replicator будет:
	// 1. читать данные с других нод;
	// 2. писать полученные данные через LocalServer в эту ноду.
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	// Membership получает Replicator как Handler.
	// Поэтому события Serf Join/Leave превращаются в
	// Replicator.Join()/Replicator.Leave().
	a.membership, err = discovery.New(
		a.replicator,
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

	// Не даём остановить Agent повторно.
	if a.shutdown {
		return nil
	}

	a.shutdown = true

	// Сигнал остальным частям программы:
	// Agent начал завершение работы.
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,

		func() error {
			a.server.GracefulStop()
			return nil
		},

		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
