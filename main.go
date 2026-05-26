// === FILE: main.go ===
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	configPath = flag.String("config", "config.yaml", "path to configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create relay
	relay, err := NewRelay(cfg)
	if err != nil {
		log.Fatalf("Failed to create relay: %v", err)
	}
	defer relay.Close()

	// Start listener
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("Failed to start listener on %s: %v", cfg.ListenAddr, err)
	}
	defer listener.Close()

	log.Printf("[MAIN] ICAP Relay starting on %s", cfg.ListenAddr)
	log.Printf("[MAIN] Backend A: %s (priority: %s)", cfg.BackendA, cfg.Priority)
	log.Printf("[MAIN] Backend B: %s", cfg.BackendB)
	log.Printf("[MAIN] Timeout: %dms, Max connections: %d", cfg.TimeoutMs, cfg.MaxConns)

	var wg sync.WaitGroup
	shutdown := make(chan struct{})

	// Signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("[MAIN] Shutdown signal received, stopping...")
		close(shutdown)
	}()

	// Accept connections
	go func() {
		for {
			select {
			case <-shutdown:
				listener.Close()
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-shutdown:
						return
					default:
						log.Printf("[MAIN] Accept error: %v", err)
						time.Sleep(100 * time.Millisecond)
						continue
					}
				}

				wg.Add(1)
				go func(c net.Conn) {
					defer wg.Done()
					relay.HandleClient(c)
				}(conn)
			}
		}
	}()

	// Wait for shutdown
	<-shutdown

	// Wait for active connections to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[MAIN] All connections closed gracefully")
	case <-time.After(10 * time.Second):
		log.Println("[MAIN] Timeout waiting for connections to close")
	}

	log.Println("[MAIN] ICAP Relay stopped")
}
