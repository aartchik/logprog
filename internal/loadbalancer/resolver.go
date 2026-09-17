package loadbalance
import (
	"context"
	"fmt"
	"sync"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	api "logprog/api/v1"
)

type Resolver struct {
    mu sync.Mutex

    // Соединение, которое создал пользователь через grpc.Dial().
    // Через него Resolver сообщает gRPC:
    // "Вот актуальный список серверов".
    clientConn resolver.ClientConn

    // Отдельное соединение самого Resolver с сервером.
    // Нужно, чтобы вызвать наш RPC GetServers().
    resolverConn *grpc.ClientConn

    // Конфигурация load balancing.
    serviceConfig *serviceconfig.ParseResult

    logger *zap.Logger
}


var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(
    target resolver.Target,
    cc resolver.ClientConn,
    opts resolver.BuildOptions,
) (resolver.Resolver, error) {

    r.logger = zap.L().Named("resolver")

    // gRPC даёт нашему resolver способ обновлять
    // известный gRPC список серверов.
    r.clientConn = cc

    var dialOpts []grpc.DialOption

    // Если клиент использует TLS,
    // resolver тоже должен использовать эти credentials,
    // когда сам подключается к Proglog.
    if opts.DialCreds != nil {
        dialOpts = append(
            dialOpts,
            grpc.WithTransportCredentials(opts.DialCreds),
        )
    }

    // Говорим gRPC использовать наш load balancer "proglog".
    r.serviceConfig = r.clientConn.ParseServiceConfig(
        fmt.Sprintf(
            `{"loadBalancingConfig":[{"%s":{}}]}`,
            Name,
        ),
    )

    var err error

    // Создаём ОТДЕЛЬНОЕ соединение resolver -> Proglog,
    // чтобы resolver мог вызвать GetServers().
    r.resolverConn, err = grpc.Dial(
        target.Endpoint(),
        dialOpts...,
    )
    if err != nil {
        return nil, err
    }

    // Сразу запускаем первое обнаружение серверов.
    r.ResolveNow(resolver.ResolveNowOptions{})

    return r, nil
}

const Name = "proglog"

func (r *Resolver) Scheme() string {
    return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// resolverConn был создан в Build().
	// Через него Resolver сам обращается к одной из нод Proglog.
	client := api.NewLogClient(r.resolverConn)

	ctx := context.Background()

	// Спрашиваем у ноды:
	// "Какие серверы сейчас входят в Raft-кластер
	// и кто из них leader?"
	res, err := client.GetServers(
		ctx,
		&api.GetServersRequest{},
	)
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}

	var addrs []resolver.Address

	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,

			// Дополнительная информация для будущего Picker.
			// Он сможет отличить leader от followers.
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}

	// Сообщаем gRPC:
	// "Вот актуальный список серверов.
	// Используй их для клиентских RPC."
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}