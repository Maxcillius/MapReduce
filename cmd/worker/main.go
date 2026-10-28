package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/maxcillius/MapReducer/worker"
)

func main() {
	coordinatorAddress := flag.String("coordinator", "", "coordinator RPC address")
	workerID := flag.Int("id", -1, "unique worker ID for this job")
	flag.Parse()

	if *coordinatorAddress == "" {
		fatal("provide the coordinator address with -coordinator")
	}
	if *workerID < 0 {
		fatal("provide a unique non-negative worker ID with -id")
	}

	for {
		if err := worker.Run(*coordinatorAddress, *workerID, ""); err == nil {
			return
		} else {
			fmt.Fprintf(os.Stderr, "worker %d lost coordinator connection: %v; reconnecting shortly\n", *workerID, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
