package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "logprog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("hello world"),
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// Записываем 3 записи и читаем их обратно.
	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)

		require.Equal(t, uint64(16)+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)

		require.Equal(t, want.Value, got.Value)
	}

	// Index рассчитан только на 3 записи,
	// поэтому 4-я запись должна завершиться ошибкой.
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// Segment заполнен из-за index.
	require.True(t, s.IsMaxed())

	// Теперь проверяем ограничение store.
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	// Открываем уже существующий segment.
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// Store уже содержит 3 записи и поэтому достиг лимита.
	require.True(t, s.IsMaxed())

	// Удаляем segment вместе с его файлами.
	err = s.Remove()
	require.NoError(t, err)

	// Создаём segment заново.
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// Теперь он пустой.
	require.False(t, s.IsMaxed())
}
