package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/maxcillius/MapReducer/coordinator"
	"github.com/maxcillius/MapReducer/worker"
)

func main() {
	inputFlag := flag.String("f", "", "comma-separated input files")
	reduceCount := flag.Int("n", 1, "number of reduce tasks")
	outputPrefix := flag.String("o", "mr-out", "output file prefix")
	workerCount := flag.Int("w", 4, "number of local workers")
	chunkMiB := flag.Int("chunk", 16, "maximum input chunk size in MiB")
	listenAddress := flag.String("listen", "127.0.0.1:9000", "coordinator RPC listen address")
	workRoot := flag.String("work-root", "./mr-work", "shared directory visible at the same path to all workers")
	checkpointFlag := flag.String("checkpoint", "", "coordinator checkpoint file")
	resume := flag.Bool("resume", false, "resume the checkpointed job")
	flag.Parse()

	if !*resume && *inputFlag == "" {
		fatal(errors.New("provide input files with -f"))
	}
	if *reduceCount < 1 || *workerCount < 0 || *chunkMiB < 1 {
		fatal(errors.New("reduce count and chunk size must be positive; worker count cannot be negative"))
	}

	var inputs []string
	if !*resume {
		inputs = strings.Split(*inputFlag, ",")
		for i := range inputs {
			inputs[i] = strings.TrimSpace(inputs[i])
			if inputs[i] == "" {
				fatal(errors.New("input file list contains an empty path"))
			}
			if _, err := os.Stat(inputs[i]); err != nil {
				fatal(fmt.Errorf("input %q: %w", inputs[i], err))
			}
		}
	}

	if err := os.MkdirAll(*workRoot, 0755); err != nil {
		fatal(fmt.Errorf("create shared work root: %w", err))
	}
	absWorkRoot, err := filepath.Abs(*workRoot)
	if err != nil {
		fatal(fmt.Errorf("resolve shared work root: %w", err))
	}
	*workRoot = absWorkRoot
	checkpointPath := *checkpointFlag
	if checkpointPath == "" {
		checkpointPath = filepath.Join(*workRoot, "mapreduce-checkpoint.json")
	}
	checkpointPath, err = filepath.Abs(checkpointPath)
	if err != nil {
		fatal(fmt.Errorf("resolve checkpoint path: %w", err))
	}

	var c *coordinator.Coordinator
	if *resume {
		c, err = coordinator.Resume(checkpointPath)
		if err != nil {
			fatal(fmt.Errorf("resume job: %w", err))
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(*outputPrefix), 0755); err != nil {
			fatal(fmt.Errorf("create output directory: %w", err))
		}
		splitDir, err := os.MkdirTemp(*workRoot, ".mapreduce-splits-*")
		if err != nil {
			fatal(fmt.Errorf("create split directory in %q: %w", *workRoot, err))
		}
		if err := os.Chmod(splitDir, 02775); err != nil {
			fatal(fmt.Errorf("set split directory permissions: %w", err))
		}
		splits, err := splitInputs(inputs, splitDir, *chunkMiB*1024*1024)
		if err != nil {
			fatal(err)
		}
		c, err = coordinator.NewPersistent(splits, *reduceCount, *outputPrefix, *workRoot, checkpointPath, splitDir)
		if err != nil {
			fatal(err)
		}
	}
	if c.InputTempDir() != "" {
		defer os.RemoveAll(c.InputTempDir())
	}
	defer os.RemoveAll(c.WorkDir())

	address, closeServer, err := c.Listen(*listenAddress)
	if err != nil {
		fatal(fmt.Errorf("start coordinator: %w", err))
	}
	defer closeServer()
	fmt.Printf("Coordinator RPC listening at %s\n", address)

	var workers sync.WaitGroup
	workerErrors := make(chan error, *workerCount)
	for id := 0; id < *workerCount; id++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			workerErrors <- worker.Run(address, id, c.WorkDir())
		}(id)
	}
	workers.Wait()
	close(workerErrors)
	for workerErr := range workerErrors {
		if workerErr != nil {
			fmt.Fprintf(os.Stderr, "local worker exited: %v\n", workerErr)
		}
	}
	<-c.Done()
	if err := c.Err(); err != nil {
		fatal(err)
	}
	fmt.Printf("MapReduce complete; output prefix: %s\n", *outputPrefix)
}

func splitInputs(inputs []string, dir string, maxBytes int) ([]string, error) {
	if maxBytes < 1 {
		return nil, errors.New("maximum split size must be positive")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create split directory: %w", err)
	}

	var paths []string
	var file *os.File
	var writer *bufio.Writer
	chunkSize := 0
	closeChunk := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		file = nil
		writer = nil
		return nil
	}
	openChunk := func() error {
		path := filepath.Join(dir, fmt.Sprintf("input-%05d.txt", len(paths)))
		var err error
		file, err = os.Create(path)
		if err != nil {
			return err
		}
		writer = bufio.NewWriter(file)
		chunkSize = 0
		paths = append(paths, path)
		return nil
	}

	for _, input := range inputs {
		f, err := os.Open(input)
		if err != nil {
			return nil, fmt.Errorf("open input %q: %w", input, err)
		}
		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanWords)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			word := scanner.Text()
			if writer == nil || (chunkSize > 0 && chunkSize+len(word)+1 > maxBytes) {
				if err := closeChunk(); err != nil {
					f.Close()
					return nil, err
				}
				if err := openChunk(); err != nil {
					f.Close()
					return nil, err
				}
			}
			if _, err := writer.WriteString(word + " "); err != nil {
				f.Close()
				return nil, err
			}
			chunkSize += len(word) + 1
		}
		scanErr := scanner.Err()
		closeErr := f.Close()
		if scanErr != nil {
			return nil, fmt.Errorf("read input %q: %w", input, scanErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if err := closeChunk(); err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, errors.New("input files contain no words")
	}
	return paths, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
