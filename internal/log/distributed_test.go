package log_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	api "logprog/api/v1"
	"logprog/internal/log"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog

	nodeCount := 3

	// Получаем 3 свободных порта:
	// по одному для каждой Raft-ноды.
	ports := dynaport.Get(nodeCount)

	for i := 0; i < nodeCount; i++ {

		// У каждой ноды свой отдельный диск/директория.
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)

		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		// Каждая нода слушает свой TCP-порт.
		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[i]),
		)
		require.NoError(t, err)

		config := log.Config{}

		// Сетевой слой, через который Raft будет общаться
		// с другими Raft-нодами.
		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)

		// Уникальный ID ноды: "0", "1", "2".
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))

		// Маленькие timeout'ы только для ускорения теста.
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.BindAddr = ln.Addr().String()

		// Только первая нода создаёт первоначальный Raft-кластер.
		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			// Ноды 1 и 2 добавляем в Raft-кластер через лидера.
			err = logs[0].Join(
				fmt.Sprintf("%d", i),
				ln.Addr().String(),
			)
			require.NoError(t, err)
		} else {
			// Для первой ноды ждём, пока она станет лидером.
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		logs = append(logs, l)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {

		// Пишем ТОЛЬКО лидеру.
		off, err := logs[0].Append(record)
		require.NoError(t, err)

		// Ждём, пока запись появится на всех трёх нодах.
		require.Eventually(t, func() bool {

			for j := 0; j < nodeCount; j++ {

				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}

				record.Offset = off

				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}

			return true

		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// Удаляем node 1 из Raft-кластера.
	err := logs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// После удаления пишем ещё одну запись лидеру.
	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Node 1 больше не должна получить "third".
	record, err := logs[1].Read(off)

	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	// Node 2 всё ещё член Raft-кластера,
	// поэтому должна получить "third".
	record, err = logs[2].Read(off)

	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
