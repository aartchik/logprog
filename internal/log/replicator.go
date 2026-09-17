package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "logprog/api/v1"
)

type Replicator struct {
	// Настройки подключения к другим gRPC-серверам.
	// Сюда, например, передадим TLS credentials.
	DialOptions []grpc.DialOption

	// gRPC-клиент НАШЕГО локального сервера.
	// Через него будем записывать скопированные записи себе.
	LocalServer api.LogClient

	logger *zap.Logger

	// Защищает поля ниже от одновременного доступа goroutine.
	mu sync.Mutex

	// Какие серверы мы сейчас реплицируем.
	// name -> канал для остановки репликации этого сервера.
	servers map[string]chan struct{}

	closed bool

	// Канал для остановки Replicator целиком.
	close chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// Этот сервер уже реплицируем.
		return nil
	}

	// Создаём персональный stop-канал для этой ноды.
	r.servers[name] = make(chan struct{})

	// В фоне начинаем копировать данные с удалённой ноды.
	go r.replicate(addr, r.servers[name])

	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	// Подключаемся к удалённой ноде, например B.
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	// Создаём gRPC-клиента удалённой ноды B.
	client := api.NewLogClient(cc)

	ctx := context.Background()

	// Просим B отдавать нам записи начиная с offset = 0.
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	// Через этот канал одна goroutine будет передавать
	// полученные от B записи основной goroutine.
	records := make(chan *api.Record)

	go func() {
		for {
			// Ждём следующую запись от B по gRPC stream.
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}

			records <- recv.Record
		}
	}()

	for {
		select {
		// Остановить Replicator целиком.
		case <-r.close:
			return

		// Остановить репликацию конкретно этой ноды.
		case <-leave:
			return

		// Пришла новая запись от B.
		case record := <-records:
			// Записываем её в наш локальный сервер A.
			_, err = r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}

	// Сигнализируем replicate(), что эту ноду больше читать не надо.
	close(r.servers[name])

	delete(r.servers, name)

	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	// После этого новые Join() будут игнорироваться.
	r.closed = true

	// Останавливаем ВСЕ запущенные replicate() goroutine.
	close(r.close)

	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
