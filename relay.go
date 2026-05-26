// === FILE: relay.go ===
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BackendResult holds the result from a backend ICAP server
type BackendResult struct {
	Response *ICAPResponse
	Error    error
	Backend  string
}

// Relay handles ICAP request relaying to backends
type Relay struct {
	config      *Config
	logger      *log.Logger
	connPoolA   *ConnectionPool
	connPoolB   *ConnectionPool
	semaphore   chan struct{}
}

// ConnectionPool manages keep-alive connections to a backend
type ConnectionPool struct {
	addr     string
	conns    chan net.Conn
	maxConns int
	mu       sync.Mutex
	closed   bool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(addr string, maxConns int) *ConnectionPool {
	return &ConnectionPool{
		addr:     addr,
		conns:    make(chan net.Conn, maxConns),
		maxConns: maxConns,
	}
}

// GetConn gets a connection from the pool or creates a new one
func (p *ConnectionPool) GetConn(ctx context.Context, timeout time.Duration) (net.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	p.mu.Unlock()

	// Try to get existing connection
	select {
	case conn := <-p.conns:
		// Check if connection is still alive
		if conn != nil {
			return conn, nil
		}
	default:
	}

	// Create new connection with timeout
	dialer := &net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, "tcp", p.addr)
}

// PutConn returns a connection to the pool
func (p *ConnectionPool) PutConn(conn net.Conn) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed || conn == nil {
		if conn != nil {
			conn.Close()
		}
		return
	}

	select {
	case p.conns <- conn:
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

// Close closes the connection pool
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return
	}
	p.closed = true
	
	close(p.conns)
	for conn := range p.conns {
		if conn != nil {
			conn.Close()
		}
	}
}

// NewRelay creates a new ICAP relay
func NewRelay(cfg *Config) (*Relay, error) {
	logger := log.New(log.Writer(), "[ICAP-RELAY] ", log.LstdFlags)
	
	// Parse backend addresses
	addrA, err := parseBackendAddress(cfg.BackendA)
	if err != nil {
		return nil, fmt.Errorf("invalid backend A address: %w", err)
	}
	addrB, err := parseBackendAddress(cfg.BackendB)
	if err != nil {
		return nil, fmt.Errorf("invalid backend B address: %w", err)
	}

	relay := &Relay{
		config:      cfg,
		logger:      logger,
		connPoolA:   NewConnectionPool(addrA, cfg.MaxConns),
		connPoolB:   NewConnectionPool(addrB, cfg.MaxConns),
		semaphore:   make(chan struct{}, cfg.MaxConns),
	}

	return relay, nil
}

// parseBackendAddress extracts host:port from ICAP URL
func parseBackendAddress(rawURL string) (string, error) {
	u, err := parseICAPURL(rawURL)
	if err != nil {
		return "", err
	}
	host := u.Host
	if host == "" {
		host = u.Path
	}
	if !strings.Contains(host, ":") {
		host = host + ":1344"
	}
	return host, nil
}

// HandleClient handles a client connection
func (r *Relay) HandleClient(clientConn net.Conn) {
	defer clientConn.Close()

	// Acquire semaphore for connection limiting
	select {
	case r.semaphore <- struct{}{}:
		defer func() { <-r.semaphore }()
	default:
		r.log("error", "max connections reached, rejecting")
		return
	}

	reader := bufio.NewReader(clientConn)
	writer := bufio.NewWriter(clientConn)
	defer writer.Flush()

	// Create context that cancels when client disconnects
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor connection for read errors (client disconnect)
	go func() {
		buf := make([]byte, 1)
		clientConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := clientConn.Read(buf)
		if err != nil && err != io.EOF {
			cancel()
		}
	}()

	// Parse ICAP request
	req, err := ParseICAPRequest(reader)
	if err != nil {
		r.log("error", "failed to parse ICAP request: %v", err)
		return
	}

	r.log("debug", "received ICAP request: %s %s", req.Method, req.URI)

	// Handle OPTIONS method specially
	if req.Method == "OPTIONS" {
		r.handleOptions(clientConn, writer, req, ctx)
		return
	}

	// Fan-out: send to both backends in parallel
	timeout := time.Duration(r.config.TimeoutMs) * time.Millisecond
	resultChan := make(chan *BackendResult, 2)

	var wg sync.WaitGroup
	
	// Determine priority
	priorityBackend := r.config.Priority
	secondaryBackend := "B"
	if priorityBackend == "B" {
		secondaryBackend = "A"
	}

	// Launch goroutines for both backends
	wg.Add(2)
	
	go func() {
		defer wg.Done()
		resp, err := r.sendToBackend(ctx, priorityBackend, req, timeout)
		resultChan <- &BackendResult{
			Response: resp,
			Error:    err,
			Backend:  priorityBackend,
		}
	}()

	go func() {
		defer wg.Done()
		resp, err := r.sendToBackend(ctx, secondaryBackend, req, timeout)
		resultChan <- &BackendResult{
			Response: resp,
			Error:    err,
			Backend:  secondaryBackend,
		}
	}()

	// Fan-in: collect results with priority logic
	var priorityResult *BackendResult
	var secondaryResult *BackendResult
	priorityReceived := false

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			result := <-resultChan
			if result.Backend == priorityBackend {
				priorityResult = result
				priorityReceived = true
			} else {
				secondaryResult = result
			}
		}
	}()

	// Wait for priority result or timeout
	select {
	case <-done:
		// Both results received
	case <-time.After(timeout + 500*time.Millisecond):
		// Timeout waiting for all results
	}

	// Decide which response to use
	var finalResponse *ICAPResponse
	var finalError error

	if priorityReceived && priorityResult.Error == nil {
		// Priority backend succeeded
		finalResponse = priorityResult.Response
		r.log("debug", "using response from priority backend %s", priorityBackend)
	} else if secondaryResult != nil && secondaryResult.Error == nil {
		// Fall back to secondary backend
		finalResponse = secondaryResult.Response
		r.log("debug", "using response from secondary backend %s", secondaryBackend)
	} else if priorityResult != nil && priorityResult.Error == nil {
		// Priority received but not marked as such (edge case)
		finalResponse = priorityResult.Response
	} else if secondaryResult != nil && secondaryResult.Error == nil {
		finalResponse = secondaryResult.Response
	} else {
		// Both failed
		finalError = fmt.Errorf("both backends failed")
		if priorityResult != nil && priorityResult.Error != nil {
			r.log("error", "backend %s failed: %v", priorityBackend, priorityResult.Error)
		}
		if secondaryResult != nil && secondaryResult.Error != nil {
			r.log("error", "backend %s failed: %v", secondaryBackend, secondaryResult.Error)
		}
	}

	// Send response to client
	if finalError != nil {
		r.sendErrorResponse(writer, 502, "Bad Gateway")
	} else {
		r.sendICAPResponse(writer, finalResponse)
	}

	writer.Flush()
}

// sendToBackend sends an ICAP request to a specific backend
func (r *Relay) sendToBackend(ctx context.Context, backend string, req *ICAPRequest, timeout time.Duration) (*ICAPResponse, error) {
	var pool *ConnectionPool
	if backend == "A" {
		pool = r.connPoolA
	} else {
		pool = r.connPoolB
	}

	conn, err := pool.GetConn(ctx, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Set deadline on connection
	conn.SetDeadline(time.Now().Add(timeout))

	// Build and send request
	rawReq := BuildICAPRequest(req, req.RawBody)
	if _, err := conn.Write(rawReq); err != nil {
		pool.PutConn(nil) // Don't return broken connection
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	// Read response
	reader := bufio.NewReader(conn)
	resp, err := ParseICAPResponse(reader)
	if err != nil {
		pool.PutConn(nil)
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Return connection to pool for reuse
	pool.PutConn(conn)

	return resp, nil
}

// ParseICAPResponse parses an ICAP response from a reader
func ParseICAPResponse(r *bufio.Reader) (*ICAPResponse, error) {
	resp := &ICAPResponse{
		Headers: make(map[string]string),
		Encapsulated: make(map[string]int),
	}

	// Read status line
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read status line: %w", err)
	}
	line = strings.TrimSpace(line)
	
	// Parse: ICAP/1.0 200 OK
	parts := strings.Fields(line)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid ICAP status line: %s", line)
	}

	resp.Version = parts[0]
	statusCode := 0
	fmt.Sscanf(parts[1], "%d", &statusCode)
	resp.StatusCode = statusCode
	resp.StatusText = strings.Join(parts[2:], " ")

	// Read headers
	for {
		headerLine, err := r.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		headerLine = strings.TrimSpace(headerLine)
		if headerLine == "" {
			break
		}

		idx := strings.Index(headerLine, ":")
		if idx == -1 {
			continue
		}
		key := strings.TrimSpace(headerLine[:idx])
		value := strings.TrimSpace(headerLine[idx+1:])
		resp.Headers[key] = value

		// Parse Preview header
		if strings.EqualFold(key, "Preview") {
			resp.Preview, _ = strconv.ParseInt(value, 10, 64)
		}

		// Parse Encapsulated header
		if strings.EqualFold(key, "Encapsulated") {
			parseEncapsulated(value, resp.Encapsulated)
		}
	}

	// Read body if present
	if len(resp.Encapsulated) > 0 || resp.Headers["Transfer-Encoding"] == "chunked" {
		body, err := readChunkedBody(r, resp.Preview)
		if err != nil {
			return nil, fmt.Errorf("failed to read body: %w", err)
		}
		resp.Body = body
	}

	return resp, nil
}

// handleOptions handles ICAP OPTIONS requests
func (r *Relay) handleOptions(clientConn net.Conn, writer *bufio.Writer, req *ICAPRequest, ctx context.Context) {
	r.log("debug", "handling OPTIONS request")

	// Send OPTIONS to both backends and merge capabilities
	timeout := time.Duration(r.config.TimeoutMs) * time.Millisecond
	resultChan := make(chan *BackendResult, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		resp, err := r.sendToBackend(ctx, "A", req, timeout)
		resultChan <- &BackendResult{Response: resp, Error: err, Backend: "A"}
	}()

	go func() {
		defer wg.Done()
		resp, err := r.sendToBackend(ctx, "B", req, timeout)
		resultChan <- &BackendResult{Response: resp, Error: err, Backend: "B"}
	}()

	// Wait for at least one response
	var firstResp *ICAPResponse
	
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			result := <-resultChan
			if result.Error == nil && firstResp == nil {
				firstResp = result.Response
			}
			if result.Error != nil {
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(timeout + 500*time.Millisecond):
	}

	if firstResp != nil {
		r.sendICAPResponse(writer, firstResp)
	} else {
		r.sendErrorResponse(writer, 502, "Bad Gateway")
	}
	writer.Flush()
}

// sendICAPResponse sends an ICAP response to the client
func (r *Relay) sendICAPResponse(writer *bufio.Writer, resp *ICAPResponse) {
	BuildICAPResponse(resp, writer)
}

// sendErrorResponse sends an error response to the client
func (r *Relay) sendErrorResponse(writer *bufio.Writer, statusCode int, statusText string) {
	resp := &ICAPResponse{
		Version:    "ICAP/1.0",
		StatusCode: statusCode,
		StatusText: statusText,
		Headers: map[string]string{
			"Server": "icap-relay",
		},
	}
	BuildICAPResponse(resp, writer)
}

// log logs a message at the specified level
func (r *Relay) log(level string, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	switch strings.ToUpper(level) {
	case "DEBUG":
		if strings.ToUpper(r.config.LogLevel) == "DEBUG" {
			r.logger.Printf("[DEBUG] %s", msg)
		}
	case "ERROR":
		r.logger.Printf("[ERROR] %s", msg)
	default:
		if strings.ToUpper(r.config.LogLevel) != "ERROR" {
			r.logger.Printf("[INFO] %s", msg)
		}
	}
}

// Close closes the relay and releases resources
func (r *Relay) Close() {
	r.connPoolA.Close()
	r.connPoolB.Close()
}
