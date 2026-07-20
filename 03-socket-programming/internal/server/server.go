package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ServerConfig berisi konfigurasi server
type ServerConfig struct {
	MaxConnections int // Maksimum koneksi simultan
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration // Timeout koneksi idle
}

// DefaultConfig mengembalikan konfigurasi default
func DefaultConfig() ServerConfig {
	return ServerConfig{
		MaxConnections: 1_0000,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
	}
}

type Server struct {
	addr       string
	listener   net.Listener // Listener untuk menerima koneksi
	logger     *log.Logger
	config     ServerConfig
	handlers   map[string]CommandHandler // Handler untuk command (sederhana)
	activeConn int32                     // Counter aktif koneksi (atomic)
	wg         sync.WaitGroup            // WaitGroup untuk graceful shutdown
	quit       chan struct{}             // Channel untuk signal shutdown
	mu         sync.Mutex                // Untuk protect listener close
	closed     bool                      // Flag untuk cek sudah closed
}

// CommandHandler adalah fungsi untuk menangani perintah
type CommandHandler func(args []string) string

// Konstruktior: NewServer membuat instance server baru
func NewServer(addr string) *Server {
	s := &Server{
		addr:       addr,
		logger:     log.New(os.Stdout, "[SERVER] ", log.LstdFlags),
		config:     DefaultConfig(),
		handlers:   make(map[string]CommandHandler),
		activeConn: 0,
		quit:       make(chan struct{}),
		closed:     false,
	}

	return s
}

// NewServerWithConfig membuat server dengan konfigurasi kustom
func NewServerWithConfig(addr string, config ServerConfig) *Server {
	s := NewServer(addr)
	s.config = config
	return s
}

// RegisterHandler mendaftarkan handler untuk perintah
func (s *Server) RegisterHandler(cmd string, handler CommandHandler) {
	s.handlers[strings.ToUpper(cmd)] = handler
}

// processCommand memproses perintah dari client
// Versi sederhana: command dan args dipisahkan oleh spasi
func (s *Server) processCommand(line string) string {
	// Trim whitespace
	line = strings.TrimSpace(line)
	if line == "" {
		return "ERR empty command"
	}

	// Parse command dan args (sederhana)
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "ERR invalid command"
	}

	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	// Cari handler
	handler, exists := s.handlers[cmd]
	if !exists {
		return fmt.Sprintf("ERR unknown command '%s'", cmd)
	}

	// Eksekusi handler
	return handler(args)
}

// handleConnection menangani satu koneksi
func (s *Server) handleConnection(conn net.Conn) {
	// Tambahkan ke WaitGroup untuk gracefull shutdown
	s.wg.Add(1)
	defer s.wg.Done()

	c := &Connection{
		conn:        conn,
		remoteAddr:  conn.RemoteAddr().String(),
		idleTimeout: s.config.IdleTimeout,
		logger:      s.logger,
		done:        make(chan struct{}),
	}

	// Pastikan connection ditutup saat function selesai
	defer func() {
		c.Close()
		// Kurangi counter aktif koneksi
		atomic.AddInt32(&s.activeConn, -1)
		s.logger.Printf("Active connections: %d", atomic.LoadInt32(&s.activeConn))
	}()

	// Cek apakah shutdown sedang berlangsung
	select {
	case <-s.quit:
		s.logger.Printf("Shutting down, rejecting new connection from %s", c.remoteAddr)
		conn.Write([]byte("ERR server shutting down\n"))
		return
	default:
	}

	// Start idle monitor
	c.UpdateActivity()
	go c.MonitorIdle()

	// Log koneksi baru
	s.logger.Printf("New connection from %s", c.remoteAddr)
	s.logger.Printf("Active connections: %d", atomic.LoadInt32(&s.activeConn))

	// Buat reader untuk membaca data
	reader := bufio.NewReaderSize(conn, 1024)

	// Channel untuk shutdown interrupt
	shutdown := make(chan struct{})
	go func() {
		<-s.quit
		c.Close() // Langsung close connection
		close(shutdown)
	}()

	// Loop membaca perintah dari client
	for {
		// Cek shutdown signal
		select {
		case <-s.quit:
			s.logger.Printf("Shutting down, closing connection from %s", c.remoteAddr)
			return
		default:
		}

		// Set read timeout
		if s.config.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}

		// Baca di goroutine terpisah dengan channel
		type readResult struct {
			line string
			err  error
		}
		readCh := make(chan readResult, 1)
		go func() {
			line, err := reader.ReadString('\n')
			readCh <- readResult{line, err}
		}()

		// Wait for read OR shutdown
		select {
		case <-shutdown:
			s.logger.Printf("Shutdown interrupt during read from %s", c.remoteAddr)
			return
		case res := <-readCh:

			if res.err != nil {
				// EOF berarti client disconnect
				if res.err == io.EOF {
					s.logger.Printf("Client %s disconnected", c.remoteAddr)
				} else if ne, ok := res.err.(net.Error); ok && ne.Timeout() {
					s.logger.Printf("Client %s timeout", c.remoteAddr)
				} else {
					s.logger.Printf("Read error from %s: %v", c.remoteAddr, res.err)
				}
				return
			}

			// Trim newline dan carriage return
			line := strings.TrimRight(res.line, "\r\n")

			// Log command (untuk debug)
			s.logger.Printf("Command from %s: %s", c.remoteAddr, line)

			// Proses command
			response := s.processCommand(line)

			// Set write timeout
			if s.config.WriteTimeout > 0 {
				conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			}

			// Kirim response (tambah newline)
			_, err := conn.Write([]byte(response + "\n"))
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					s.logger.Printf("Write timeout to %s", c.remoteAddr)
				} else {
					s.logger.Printf("Write error to %s: %v", c.remoteAddr, err)
				}
				return
			}
			// Update per-koneksi activity
			c.UpdateActivity()
		}
	}
}

// Start memulai server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	s.logger.Printf("Server listening on %s", s.addr)
	s.logger.Printf("Max connections: %d", s.config.MaxConnections)
	s.logger.Printf("Read timeout: %v", s.config.ReadTimeout)
	s.logger.Printf("Write timeout: %v", s.config.WriteTimeout)
	s.logger.Printf("Idle timeout: %v", s.config.IdleTimeout)

	for {
		// Cek apakah ada sinyal shutdown
		select {
		case <-s.quit:
			s.logger.Println("Server stopped")
			return nil
		default:
		}

		// Terima koneksi
		conn, err := s.listener.Accept()
		if err != nil {
			// Cek apakah listener sengaja ditutup
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()

			if closed {
				s.logger.Println("Listener closed, stopping accept loop")
				return nil
			}

			// Cek apakah error karena listener ditutup (shutdown)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}

			s.logger.Printf("Accept error: %v", err)
			continue
		}

		// Cek shutdown signal sebelum menerima koneksi baru
		select {
		case <-s.quit:
			conn.Write([]byte("ERR server shutting down\n"))
			conn.Close()
			continue
		default:
		}

		// Cek apakah sudah mencapai batas maksimal koneksi
		currentConn := atomic.LoadInt32(&s.activeConn)
		if currentConn >= int32(s.config.MaxConnections) {
			// Tolak koneksi dengan pesan error
			s.logger.Printf("Max connections reached (%d), rejecting connection from %s",
				s.config.MaxConnections, conn.RemoteAddr().String())
			conn.Write([]byte("ERR server full, maximum connections reached\n"))
			conn.Close()
			continue
		}

		// Tambah counter aktif koneksi
		atomic.AddInt32(&s.activeConn, 1)

		go s.handleConnection(conn)
	}
}

// Shutdown menghentikan server dengan graceful
func (s *Server) Shutdown() error {
	return s.ShutdownWithTimeout(30 * time.Second)
}

// ShutdownWithTimeout menghentikan server dengan timeout
func (s *Server) ShutdownWithTimeout(timeout time.Duration) error {
	s.logger.Println("Starting graceful shutdown...")

	// 1. Signal untuk stop menerima koneksi baru
	close(s.quit)

	// 2. Tutup listener (stop accept)
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.listener != nil {
			s.listener.Close()
		}
	}
	s.mu.Unlock()

	// 3. Tunggu semua koneksi selesai dengan timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait() // Tunggu semua goroutine selesai
		close(done)
	}()

	// 4. Tunggu atau timeout
	select {
	case <-done:
		s.logger.Println("All connections finished gracefully")
		return nil
	case <-time.After(timeout):
		active := atomic.LoadInt32(&s.activeConn)
		s.logger.Printf("Shutdown timeout after %v, %d connections still active",
			timeout, active)
		return fmt.Errorf("shutdown timeout after %v, %d connections still active",
			timeout, active)
	}
}

func (s *Server) PrintMemoryUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Konversi ke MB dengan 2 desimal
	allocKB := float64(m.Alloc) / 1024
	totalAllocKB := float64(m.TotalAlloc) / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	return fmt.Sprintf(
		`{"alloc": "%.2f KB", "total_alloc": "%.2f KB", "sys": "%.2f MB", "num_gc": "%d"}`,
		allocKB,
		totalAllocKB,
		sysMB,
		m.NumGC,
	)
}
