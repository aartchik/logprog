package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "logprog/api/v1"
	"logprog/internal/agent"
	"logprog/internal/config"
)

func TestAgent(t *testing.T) {
	// TLS-конфиг для gRPC-сервера каждой ноды.
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// TLS-конфиг, с которым ноды подключаются друг к другу.
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent

	// Создаём кластер из трёх нод.
	for i := 0; i < 3; i++ {
		// Один порт для Serf, второй для gRPC.
		ports := dynaport.Get(2)

		bindAddr := fmt.Sprintf(
			"%s:%d",
			"127.0.0.1",
			ports[0],
		)

		rpcPort := ports[1]

		// У каждой ноды своё физическое хранилище.
		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string

		// Node 0 создаёт кластер.
		// Node 1 и Node 2 входят в кластер через Node 0.
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		a, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}

	// После теста корректно останавливаем все три ноды
	// и удаляем временные данные.
	defer func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)

			require.NoError(
				t,
				os.RemoveAll(a.Config.DataDir),
			)
		}
	}()

	// Даём Serf время обнаружить все ноды.
	time.Sleep(3 * time.Second)

	// Подключаемся к Node 0.
	leaderClient := client(
		t,
		agents[0],
		peerTLSConfig,
	)

	// Записываем "foo" ТОЛЬКО в Node 0.
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// Проверяем, что Node 0 сама может прочитать запись.
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(
		t,
		consumeResponse.Record.Value,
		[]byte("foo"),
	)

	// Даём Replicator время скопировать запись
	// с Node 0 на остальные ноды.
	time.Sleep(3 * time.Second)

	// Теперь подключаемся уже к Node 1.
	followerClient := client(
		t,
		agents[1],
		peerTLSConfig,
	)

	// И пытаемся получить ту же запись с Node 1.
	// Если получили "foo", значит репликация сработала.
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(
		t,
		consumeResponse.Record.Value,
		[]byte("foo"),
	)
}

func client(
	t *testing.T,
	a *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	// TLS для подключения к конкретной ноде.
	tlsCreds := credentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	// Получаем gRPC-адрес ноды.
	rpcAddr, err := a.Config.RPCAddr()
	require.NoError(t, err)

	// Открываем gRPC-соединение.
	conn, err := grpc.Dial(
		fmt.Sprintf("%s", rpcAddr),
		opts...,
	)
	require.NoError(t, err)

	return api.NewLogClient(conn)
}