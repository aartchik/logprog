package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	api "github.com/travisjeffery/proglog/api/v1"
)

type DistributedLog struct {
	config Config

	log  *Log        // наш старый локальный лог
	raft *raft.Raft  // Raft, который будет координировать ноды
}

func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{
		config: config,
	}

	// Создаём обычный локальный Log этой ноды.
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	// Позже здесь настроим Raft для этой ноды.
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	l.log, err = NewLog(logDir, l.config)

	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	// FSM будет применять подтверждённые Raft-команды
	// к нашему обычному пользовательскому Log.
	fsm := &fsm{log: l.log}

	// Отдельная директория для внутреннего журнала Raft.
	logDir := filepath.Join(dataDir, "raft", "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// Используем наш собственный Log как хранилище Raft-команд.
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1

	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// Постоянное служебное состояние Raft:
	// term, за кого голосовали и т.д.
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	// Хранилище snapshot'ов.
	// Оставляем последний snapshot.
	retain := 1

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	// Сетевая связь этой Raft-ноды с другими Raft-нодами.
	maxPool := 5
	timeout := 10 * time.Second

	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// Основные настройки Raft.
	config := raft.DefaultConfig()

	// Уникальный ID этой ноды.
	config.LocalID = l.config.Raft.LocalID

	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}

	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}

	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}

	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// Собираем все предыдущие компоненты
	// и наконец создаём сам Raft.
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	// Проверяем, запускалась ли эта Raft-нода раньше.
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	// Если это первая нода нового кластера,
	// создаём кластер из одной этой ноды.
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}