package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tysonmote/gommap"

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

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)

		require.Equal(t, uint64(16)+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)

		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	require.False(t, s.IsMaxed())
}

func TestSegmentRecoversPreallocatedIndex(t *testing.T) {
	dir := t.TempDir()

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		_, err = s.Append(&api.Record{Value: []byte("record")})
		require.NoError(t, err)
	}

	require.NoError(t, s.store.buf.Flush())
	require.NoError(t, s.index.mmap.Sync(gommap.MS_SYNC))

	reopened, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(18), reopened.nextOffset)

	off, err := reopened.Append(&api.Record{Value: []byte("after restart")})
	require.NoError(t, err)
	require.Equal(t, uint64(18), off)
	require.NoError(t, reopened.Close())
}
