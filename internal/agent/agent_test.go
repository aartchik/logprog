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
	"logprog/internal/loadbalancer"
)

func TestAgent(t *testing.T) {
	// TLS-конфигурация, которую сервер использует
	// для входящих клиентских соединений.
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// TLS-конфигурация для соединений между серверами
	// и для нашего тестового клиента.
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
		// Нам нужны два порта:
		//
		// ports[0] — Serf / service discovery
		// ports[1] — RPC-порт, на котором через cmux
		//            работают gRPC и Raft.
		ports := dynaport.Get(2)

		bindAddr := fmt.Sprintf(
			"%s:%d",
			"127.0.0.1",
			ports[0],
		)

		rpcPort := ports[1]

		// Каждой ноде даём отдельную временную директорию.
		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string

		// Первая нода создаёт кластер.
		//
		// Вторая и третья ноды через Serf подключаются
		// к первой ноде.
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		a, err := agent.New(agent.Config{
			NodeName:       fmt.Sprintf("%d", i),
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddr,
			RPCPort:        rpcPort,
			DataDir:        dataDir,

			ACLModelFile:  config.ACLModelFile,
			ACLPolicyFile: config.ACLPolicyFile,

			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,

			// Только первая нода bootstrap'ит Raft-кластер.
			Bootstrap: i == 0,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}

	// После завершения теста выключаем все ноды
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

	// Даём Serf/Raft время обнаружить ноды,
	// добавить их в кластер и стабилизироваться.
	time.Sleep(3 * time.Second)

	// Создаём клиент.
	//
	// ВАЖНО:
	// теперь это уже не клиент, жёстко привязанный к agents[0].
	//
	// agents[0] является стартовой точкой discovery.
	// Наш Resolver вызовет GetServers() и узнает обо всём кластере.
	leaderClient := client(
		t,
		agents[0],
		peerTLSConfig,
	)

	// Produce через Picker должен быть направлен
	// на текущего Raft leader.
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// Теперь Consume будет направлен Picker'ом
	// не на leader, а на одного из followers.
	//
	// Поэтому сначала нужно дать Raft время
	// реплицировать запись на followers.
	time.Sleep(3 * time.Second)

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

	// Создаём ещё один клиент, используя вторую ноду
	// как стартовую точку discovery.
	//
	// Resolver всё равно должен узнать весь кластер.
	followerClient := client(
		t,
		agents[1],
		peerTLSConfig,
	)

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

	// Мы записали ТОЛЬКО одну запись.
	//
	// Раньше, до Raft, серверы могли реплицировать
	// записи друг у друга циклически:
	//
	// A -> B -> A -> B -> ...
	//
	// Теперь такого быть не должно.
	//
	// Поэтому следующего offset существовать не должно.
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)

	require.Nil(t, consumeResponse)
	require.Error(t, err)

	got := grpc.Code(err)
	want := grpc.Code(
		api.ErrOffsetOutOfRange{}.
			GRPCStatus().
			Err(),
	)

	require.Equal(t, got, want)
}

// client создаёт gRPC-клиент, использующий наш собственный
// Resolver и Picker.
//
// Раньше здесь было:
//
//     grpc.Dial(rpcAddr, ...)
//
// и клиент подключался непосредственно к одной ноде.
//
// Теперь target имеет вид:
//
//     proglog:///127.0.0.1:12345
//
// "proglog" — Scheme нашего Resolver.
//
// Поэтому gRPC:
//   1. находит loadbalance.Resolver;
//   2. Resolver подключается к указанной стартовой ноде;
//   3. вызывает GetServers();
//   4. получает адреса всех серверов;
//   5. передаёт их Balancer;
//   6. Picker направляет Produce -> leader;
//   7. Picker направляет Consume -> followers.
func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(
		fmt.Sprintf(
			"%s:///%s",
			loadbalance.Name,
			rpcAddr,
		),
		opts...,
	)
	require.NoError(t, err)

	return api.NewLogClient(conn)
}