package loadbalance_test

import (
	"net"
	"testing"
	"net/url"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "logprog/api/v1"
	"logprog/internal/config"
	"logprog/internal/loadbalancer"
	"logprog/internal/server"
)

func TestResolver(t *testing.T) {
	// Поднимаем обычный TCP-listener на случайном свободном порту.
	//
	// Именно к этому серверу наш Resolver потом подключится
	// и вызовет RPC GetServers(), чтобы узнать адреса нод кластера.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// TLS-конфигурация для тестового gRPC-сервера.
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(tlsConfig)

	// Создаём тестовый gRPC-сервер.
	//
	// Вместо настоящего DistributedLog передаём mock getServers.
	// Его задача очень простая:
	// GetServers() всегда возвращает заранее известные две ноды.
	srv, err := server.NewGRPCServer(
		&server.Config{
			GetServerer: &getServers{},
		},
		grpc.Creds(serverCreds),
	)
	require.NoError(t, err)

	// Запускаем gRPC-сервер в отдельной goroutine,
	// потому что Serve() блокирует текущую goroutine.
	go srv.Serve(l)

	// Это fake реализации resolver.ClientConn.
	//
	// В реальном gRPC ClientConn принадлежит самому gRPC.
	// Resolver сообщает ему:
	//
	// "Я обнаружил вот такие серверы".
	//
	// В тесте нам настоящий ClientConn не нужен.
	// Мы просто хотим перехватить State,
	// который Resolver передаст через UpdateState().
	conn := &clientConn{}

	// Теперь создаём TLS-конфигурацию уже для клиента.
	//
	// Именно с ней Resolver подключится к тестовому
	// gRPC-серверу выше.
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(tlsConfig)

	// BuildOptions gRPC передаёт Resolver'у при его создании.
	// Здесь нас прежде всего интересуют TLS credentials.
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}

	r := &loadbalance.Resolver{}

	// Строим наш Resolver.
	//
	// Endpoint указывает на тестовый gRPC-сервер,
	// который мы подняли выше.
	//
	// Внутри Build() Resolver подключится к этому адресу,
	// вызовет ResolveNow(), а тот сделает GetServers().
	_, err = r.Build(
		resolver.Target{
			URL: url.URL{
				Path: l.Addr().String(),
			},
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	// Вот такое состояние мы ожидаем получить от Resolver.
	//
	// Mock GetServers() возвращает:
	//
	// localhost:9001 -> leader
	// localhost:9002 -> follower
	//
	// Resolver должен преобразовать api.Server
	// в resolver.Address и сохранить информацию
	// о leader/follower в Attributes.
	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr: "localhost:9001",
				Attributes: attributes.New(
					"is_leader",
					true,
				),
			},
			{
				Addr: "localhost:9002",
				Attributes: attributes.New(
					"is_leader",
					false,
				),
			},
		},
	}

	// Проверяем, что Build() действительно вызвал ResolveNow()
	// и Resolver сообщил ClientConn правильный список серверов.
	require.Equal(t, wantState, conn.state)

	// Специально очищаем сохранённые адреса.
	//
	// Сейчас хотим проверить уже непосредственно ResolveNow():
	// сможет ли Resolver повторно обнаружить серверы.
	conn.state.Addresses = nil

	r.ResolveNow(resolver.ResolveNowOptions{})

	// После повторного ResolveNow() состояние
	// снова должно совпадать с ожидаемым.
	require.Equal(t, wantState, conn.state)
}

// getServers — mock для интерфейса GetServerer.
//
// Вместо настоящего Raft-кластера он всегда возвращает
// фиксированный список из двух серверов.
type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		},
		{
			Id:       "follower",
			RpcAddr:  "localhost:9002",
			IsLeader: false,
		},
	}, nil
}

// clientConn — fake resolver.ClientConn.
//
// Настоящий resolver.ClientConn принадлежит gRPC.
// Через него Resolver сообщает gRPC:
//
// "Вот новый список доступных серверов".
//
// Нам в тесте достаточно сохранить переданный State,
// чтобы потом проверить его через require.Equal().
type clientConn struct {
	resolver.ClientConn

	state resolver.State
}

// UpdateState вызывается нашим Resolver вот здесь:
//
//	r.clientConn.UpdateState(resolver.State{...})
//
// Мы просто сохраняем полученное состояние,
// чтобы TestResolver мог его проверить.
func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

// Остальные методы нужны для реализации resolver.ClientConn.
// В данном тесте они нам фактически не нужны.

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(
	config string,
) *serviceconfig.ParseResult {
	return nil
}