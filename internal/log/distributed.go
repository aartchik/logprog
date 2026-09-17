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

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"

	api "logprog/api/v1"
)

type DistributedLog struct {
	config Config

	log  *Log       // наш старый локальный лог
	raft *raft.Raft // Raft, который будет координировать ноды
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

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(
	reqType RequestType,
	req proto.Message,
) (interface{}, error) {

	var buf bytes.Buffer

	// Первый байт говорит FSM, КАКУЮ операцию нужно выполнить.
	// Сейчас у нас только Append, но потом могли бы быть Delete, Update и т.д.
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	// Сериализуем сам запрос в []byte.
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	buf.Write(b)

	timeout := 10 * time.Second

	// Самое важное место:
	// передаём команду Raft.
	// Raft реплицирует её и после commit передаст FSM.
	future := l.raft.Apply(buf.Bytes(), timeout)

	if future.Error() != nil {
		return nil, future.Error()
	}

	// Это результат выполнения FSM.Apply().
	res := future.Response()

	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	// Чтение пока идёт прямо из локального лога,
	// минуя Raft.
	return l.log.Read(offset)
}

// Проверка на этапе компиляции:
// *fsm обязан реализовывать интерфейс raft.FSM.
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	// Настоящий локальный пользовательский лог этой ноды.
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Raft вызывает Apply ПОСЛЕ того,
// как запись была committed.
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data

	// Первый байт мы сами ранее записали как тип команды.
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		// Всё после первого байта — protobuf ProduceRequest.
		return l.applyAppend(buf[1:])
	}

	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest

	// Восстанавливаем ProduceRequest из []byte.
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	// ВОТ ЗДЕСЬ запись наконец физически
	// добавляется в пользовательский локальный Log.
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{
		Offset: offset,
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// Получаем Reader, который позволяет последовательно
	// прочитать весь пользовательский Log.
	r := f.log.Reader()

	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	// Копируем всё состояние FSM (наш Log)
	// в хранилище snapshot'а.
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}

func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		// Читаем размер следующего Record.
		_, err := io.ReadFull(r, b)

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))

		// Читаем сам сериализованный Record.
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}

		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		// Первый Record определяет начальный offset
		// восстанавливаемого Log.
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset

			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		if _, err = f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}

	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

// Compile-time проверка:
// *StreamLayer должен реализовывать raft.StreamLayer.
var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	// TCP listener для входящих соединений.
	ln net.Listener

	// TLS для тех, кто подключается К НАМ.
	serverTLSConfig *tls.Config

	// TLS для соединений, которые МЫ открываем к другим Raft-нодам.
	peerTLSConfig *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {

	dialer := &net.Dialer{
		Timeout: timeout,
	}

	// Открываем обычное TCP-соединение с другой нодой.
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// Первый байт говорит принимающей стороне:
	// "это соединение предназначено для Raft".
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	// После определения типа соединения оборачиваем TCP в TLS.
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, nil
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	// Ждём входящее TCP-соединение.
	// Блокируется, пока кто-нибудь не подключится.
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	// Первый байт соединения используется как идентификатор протокола.
	b := make([]byte, 1)

	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	// Dial() на другой ноде первым отправляет RaftRPC = 1.
	// Проверяем, действительно ли это Raft-соединение.
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	// Теперь, когда поняли, что это Raft,
	// превращаем обычное TCP-соединение в TLS-соединение.
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	// Перестаём принимать новые соединения.
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	// Возвращаем адрес, на котором слушает StreamLayer.
	return s.ln.Addr()
}

func (l *DistributedLog) Join(id, addr string) error {
	// Узнаём текущий состав Raft-кластера.
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {

		// Есть конфликт по ID или адресу.
		if srv.ID == serverID || srv.Address == serverAddr {

			// Такой сервер уже существует ровно с этими ID + addr.
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}

			// Например ID тот же, а адрес поменялся.
			// Удаляем старую запись.
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	// Добавляем новую ноду как голосующего участника Raft.
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

func (l *DistributedLog) Leave(id string) error {
	// Удаляем ноду из состава Raft-кластера.
	removeFuture := l.raft.RemoveServer(
		raft.ServerID(id),
		0,
		0,
	)

	return removeFuture.Error()
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutC := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutC:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l.raft.Leader() != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	if err := l.raft.Shutdown().Error(); err != nil {
		return err
	}
	return l.log.Close()
}


func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	// Просим Raft дать текущую конфигурацию кластера.
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	var servers []*api.Server

	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:      string(server.ID),
			RpcAddr: string(server.Address),

			// Raft знает адрес текущего лидера.
			IsLeader: l.raft.Leader() == server.Address,
		})
	}

	return servers, nil
}