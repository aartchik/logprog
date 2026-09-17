package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

// Проверяем на этапе компиляции,
// что *Picker реализует интерфейс base.PickerBuilder.
var _ base.PickerBuilder = (*Picker)(nil)

type Picker struct {
	mu sync.RWMutex

	// Соединение с leader.
	// Все Produce-запросы будем отправлять сюда.
	leader balancer.SubConn

	// Соединения с followers.
	// Consume-запросы будем распределять между ними.
	followers []balancer.SubConn

	// Счётчик для round-robin между followers.
	current uint64
}

// Build вызывается gRPC, когда у него появились готовые соединения
// с серверами, которые ранее обнаружил Resolver.
//
// Здесь мы разделяем соединения:
// leader отдельно, followers отдельно.
func (p *Picker) Build(
	buildInfo base.PickerBuildInfo,
) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn

	// ReadySCs содержит уже готовые соединения с серверами.
	for sc, scInfo := range buildInfo.ReadySCs {

		// Resolver раньше положил в Address.Attributes:
		//
		// "is_leader" -> true/false
		//
		// Теперь Picker эту информацию достаёт.
		isLeader := scInfo.
			Address.
			Attributes.
			Value("is_leader").(bool)

		if isLeader {
			// Запоминаем соединение с leader.
			p.leader = sc
			continue
		}

		// Все остальные соединения считаем followers.
		followers = append(followers, sc)
	}

	p.followers = followers

	return p
}

// Проверяем, что *Picker реализует balancer.Picker.
var _ balancer.Picker = (*Picker)(nil)

// Pick вызывается gRPC перед конкретным RPC.
//
// Именно здесь решается:
// "На какой сервер отправить этот запрос?"
func (p *Picker) Pick(
	info balancer.PickInfo,
) (balancer.PickResult, error) {

	p.mu.RLock()
	defer p.mu.RUnlock()

	var result balancer.PickResult

	// Produce должен идти только leader.
	//
	// Если followers вообще нет,
	// тоже используем leader.
	if strings.Contains(info.FullMethodName, "Produce") ||
		len(p.followers) == 0 {

		result.SubConn = p.leader

	} else if strings.Contains(info.FullMethodName, "Consume") {

		// Consume можно выполнять на follower,
		// поэтому выбираем очередного follower через round-robin.
		result.SubConn = p.nextFollower()
	}

	// Если подходящего соединения сейчас нет,
	// сообщаем gRPC, что SubConn недоступен.
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}

	return result, nil
}

// nextFollower реализует round-robin.
//
// Например:
//
// followers = [B, C, D]
//
// вызовы будут примерно:
//
// B -> C -> D -> B -> C -> D -> ...
func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))

	length := uint64(len(p.followers))

	idx := int(cur % length)

	return p.followers[idx]
}

// Регистрируем наш балансировщик в gRPC.
//
// Name = "proglog"
//
// Именно это имя Resolver раньше записывал
// в service config:
//
// "loadBalancingConfig": [{"proglog": {}}]
func init() {
	balancer.Register(
		base.NewBalancerBuilder(
			Name,
			&Picker{},
			base.Config{},
		),
	)
}
