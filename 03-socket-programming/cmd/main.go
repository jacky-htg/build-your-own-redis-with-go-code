package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"pendem/internal/server"
	"syscall"
	"time"
)

func main() {
	config := server.ServerConfig{
		MaxConnections: 50_000,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
	}
	srv := server.NewServerWithConfig(":6378", config)

	srv.RegisterHandler("PING", func(args []string) string {
		if len(args) > 0 {
			return args[0]
		}
		return "PONG"
	})

	srv.RegisterHandler("MEMORY", func(args []string) string {
		if len(args) > 0 {
			return args[0]
		}

		return srv.PrintMemoryUsage()
	})

	fmt.Println("╔═══════════════════════════════════════════════════════╗")
	fmt.Println("║                     P E N D E M                       ║")
	fmt.Println("║              Simple Cache Server in Go                ║")
	fmt.Println("╠═══════════════════════════════════════════════════════╣")
	fmt.Printf("║  Address			: %-22s║\n", "0.0.0.0:6378")
	fmt.Printf("║  Max Coonection		: %-22d║\n", config.MaxConnections)
	fmt.Printf("║  Read Timeout			: %-22s║\n", config.ReadTimeout)
	fmt.Printf("║  Write Timeout		: %-22s║\n", config.WriteTimeout)
	fmt.Printf("║  Idle Timeout			: %-22s║\n", config.IdleTimeout)
	fmt.Println("╚═══════════════════════════════════════════════════════╝")
	fmt.Println()

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Gracefull shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-quit
	fmt.Printf("\n\n╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  Received signal: %-34s ║\n", sig)
	fmt.Printf("║  Shutting down gracefully...                         ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n")

	if err := srv.Shutdown(); err != nil {
		log.Printf("Shutdown error: %v", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ Server stopped gracefully")
}
