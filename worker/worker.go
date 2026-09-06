package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

type Args struct {
	Filename string
}

type Worker int

func (w *Worker) Reduce(args *Args, reply *string) error {
	return nil
}

func (w *Worker) Map(args *Args, reply *string) error {
	frequency := make(map[string]int)

	f, err := os.Open(fmt.Sprintf("../split/%v", args.Filename))
	if err != nil {
		// Use dedicated log
		fmt.Println("File opening:", err)
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)
	if err = scanner.Err(); err != nil {
		fmt.Println("Scanner:", err)
		return err
	}
	for scanner.Scan() {
		txt := scanner.Text()
		frequency[txt]++
	}
	if err = scanner.Err(); err != nil {
		fmt.Println("Scanner:", err)
		return err
	}

	json, err := json.Marshal(frequency)
	if err != nil {
		fmt.Println("Json marshal:", err)
		return err
	}
	// Handle mid failure
	err = os.WriteFile(fmt.Sprintf("../intermediate/%v.json", args.Filename), json, 0644)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	worker := new(Worker)
	rpc.Register(worker)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	errChn := make(chan error, 1)

	go func() {
		fmt.Printf("Listening on: %d\n", 8000)
		errChn <- http.Serve(l, nil)
	}()

	select {
	case <-ctx.Done():
		// gracefull shutdown
		os.Exit(1)
	case err := <-errChn:
		log.Fatal("http failure:", err)
	}
}
