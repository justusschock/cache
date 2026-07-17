package memcached

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	cache "github.com/justusschock/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockerRejectsExpiryOutsideMemcachedUnixRange(t *testing.T) {
	client := memcache.New("127.0.0.1:0")
	client.Timeout = 10 * time.Millisecond
	locker, err := NewLocker[string](client)
	require.NoError(t, err)

	_, err = locker.Lock(context.Background(), "users", "alice", unsupportedLeaseTTL())
	assert.ErrorIs(t, err, cache.ErrInvalidTTL)
}

func TestLeaseRefreshRejectsExpiryOutsideMemcachedUnixRange(t *testing.T) {
	client := memcache.New("127.0.0.1:0")
	client.Timeout = 10 * time.Millisecond
	lease := &memcachedLease{client: client, key: "lease", token: "owner"}

	err := lease.Refresh(context.Background(), unsupportedLeaseTTL())
	assert.ErrorIs(t, err, cache.ErrInvalidTTL)
}

func TestLeaseReleaseCannotDeleteSuccessorAfterTombstoneCAS(t *testing.T) {
	server := newReleaseRaceServer(t)
	lease := &memcachedLease{
		client: memcache.New(server.address()),
		key:    "lease",
		token:  "owner",
	}

	require.NoError(t, lease.Release(context.Background()))

	value, present, deleteSeen := server.state()
	assert.True(t, present)
	assert.Equal(t, "successor", value)
	assert.False(t, deleteSeen)
}

func unsupportedLeaseTTL() time.Duration {
	ttl := time.Until(time.Unix(int64(math.MaxInt32)+1, 0))
	const maximumRelativeTTL = 30 * 24 * time.Hour
	if ttl <= maximumRelativeTTL {
		return maximumRelativeTTL + time.Hour
	}
	return ttl
}

type releaseRaceServer struct {
	listener net.Listener

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	value       string
	present     bool
	cas         uint64
	deleteSeen  bool
	wg          sync.WaitGroup
}

func newReleaseRaceServer(t *testing.T) *releaseRaceServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &releaseRaceServer{
		listener:    listener,
		connections: make(map[net.Conn]struct{}),
		value:       "owner",
		present:     true,
		cas:         1,
	}
	server.wg.Add(1)
	go server.serve()
	t.Cleanup(server.close)
	return server
}

func (s *releaseRaceServer) address() string {
	return s.listener.Addr().String()
}

func (s *releaseRaceServer) state() (string, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.present, s.deleteSeen
}

func (s *releaseRaceServer) close() {
	_ = s.listener.Close()
	s.mu.Lock()
	for connection := range s.connections {
		_ = connection.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *releaseRaceServer) serve() {
	defer s.wg.Done()
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.connections[connection] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConnection(connection)
	}
}

func (s *releaseRaceServer) serveConnection(connection net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.connections, connection)
		s.mu.Unlock()
		_ = connection.Close()
	}()
	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return
		}
		switch fields[0] {
		case "gets":
			s.writeCurrent(writer, fields[1])
		case "cas":
			s.handleCAS(reader, writer, fields)
		case "delete":
			s.handleDelete(writer)
		default:
			_, _ = writer.WriteString("ERROR\r\n")
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *releaseRaceServer) writeCurrent(writer *bufio.Writer, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.present {
		_, _ = fmt.Fprintf(writer, "VALUE %s 0 %d %d\r\n%s\r\n", key, len(s.value), s.cas, s.value)
	}
	_, _ = writer.WriteString("END\r\n")
}

func (s *releaseRaceServer) handleCAS(reader *bufio.Reader, writer *bufio.Writer, fields []string) {
	if len(fields) != 6 {
		_, _ = writer.WriteString("CLIENT_ERROR malformed cas\r\n")
		return
	}
	size, err := strconv.Atoi(fields[4])
	if err != nil {
		_, _ = writer.WriteString("CLIENT_ERROR malformed size\r\n")
		return
	}
	payload := make([]byte, size+2)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return
	}

	s.mu.Lock()
	s.value = "successor"
	s.present = true
	s.cas++
	s.mu.Unlock()
	_, _ = writer.WriteString("STORED\r\n")
}

func (s *releaseRaceServer) handleDelete(writer *bufio.Writer) {
	s.mu.Lock()
	s.present = false
	s.deleteSeen = true
	s.mu.Unlock()
	_, _ = writer.WriteString("DELETED\r\n")
}
