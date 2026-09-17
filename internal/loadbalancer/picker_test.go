package loadbalance_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"logprog/internal/loadbalancer"
)

// Проверяем поведение Picker до того,
// как Resolver вообще обнаружил какие-либо серверы.
//
// Ни leader, ни followers пока нет.
// Поэтому любой RPC отправлять некуда.
func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}

	for _, method := range []string{
		"/log.vX.Log/Produce",
		"/log.vX.Log/Consume",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}

		result, err := picker.Pick(info)

		// Picker должен сказать gRPC:
		// "сейчас нет доступного соединения".
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

// Проверяем главное правило записи:
//
// Produce-запросы должны ВСЕГДА отправляться leader.
//
// В setupTest():
//
//	subConns[0] -> leader
//	subConns[1] -> follower
//	subConns[2] -> follower
func TestPickerProducesToLeader(t *testing.T) {
	picker, subConns := setupTest()

	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	}

	// Несколько раз вызываем Pick и убеждаемся,
	// что результат всегда один и тот же: leader.
	for i := 0; i < 5; i++ {
		gotPick, err := picker.Pick(info)

		require.NoError(t, err)
		require.Equal(t, subConns[0], gotPick.SubConn)
	}
}

// Проверяем правило чтения:
//
// Consume-запросы должны идти не leader,
// а распределяться между followers.
//
// Используется round-robin:
//
//	запрос 1 -> follower 1
//	запрос 2 -> follower 2
//	запрос 3 -> follower 1
//	запрос 4 -> follower 2
//	...
func TestPickerConsumesFromFollowers(t *testing.T) {
	picker, subConns := setupTest()

	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	}

	for i := 0; i < 5; i++ {
		pick, err := picker.Pick(info)

		require.NoError(t, err)

		// i%2 даёт:
		//
		// 0, 1, 0, 1, 0...
		//
		// +1 превращает это в:
		//
		// 1, 2, 1, 2, 1...
		//
		// А subConns[1] и subConns[2]
		// как раз являются followers.
		require.Equal(t, subConns[i%2+1], pick.SubConn)
	}
}

// setupTest создаёт искусственный кластер из трёх SubConn:
//
//	subConns[0] -> leader
//	subConns[1] -> follower
//	subConns[2] -> follower
//
// Никаких настоящих серверов здесь нет.
// Мы тестируем только логику Picker.
func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn

	// PickerBuildInfo — это информация,
	// которую настоящий gRPC Balancer передаёт Picker.Build().
	//
	// ReadySCs содержит готовые к использованию соединения.
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}

	for i := 0; i < 3; i++ {
		sc := &subConn{}

		// Именно такой attribute ранее устанавливал наш Resolver.
		//
		// Для i == 0:
		//     is_leader = true
		//
		// Для остальных:
		//     is_leader = false
		addr := resolver.Address{
			Attributes: attributes.New(
				"is_leader",
				i == 0,
			),
		}

		// Нулевая SubConn является leader.
		sc.UpdateAddresses([]resolver.Address{addr})

		// Сообщаем Picker.Build(), что эта SubConn готова.
		buildInfo.ReadySCs[sc] = base.SubConnInfo{
			Address: addr,
		}

		subConns = append(subConns, sc)
	}

	picker := &loadbalance.Picker{}

	// Передаём Picker наши три искусственных соединения.
	//
	// Внутри Build() он разделит их:
	//
	// leader    -> subConns[0]
	// followers -> subConns[1], subConns[2]
	picker.Build(buildInfo)

	return picker, subConns
}

// subConn — минимальный mock для balancer.SubConn.
//
// Настоящий balancer.SubConn представляет соединение gRPC
// с конкретным backend-сервером.
//
// Здесь настоящее сетевое соединение нам совершенно не нужно,
// поэтому делаем простую заглушку.
type subConn struct {
	balancer.SubConn
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}