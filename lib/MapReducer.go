package lib

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
)

func Split(directory string, chunkSize int) {
	files, err := os.ReadDir(directory)
	if err != nil {
		log.Fatal("Directory Reading:", err)
	}
	// Explicitly create split folder if doesn't exists
	size := chunkSize * 1000
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		f, err := os.Open(directory + "/" + file.Name())
		if err != nil {
			log.Fatal("File Opening:", err)
		}
		counter := 1
		defer f.Close()
		reader := bufio.NewReader(f)
		var buffer []byte
		terminate := false
		for {
			b, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					if len(buffer) > 0 {
						err := os.WriteFile(fmt.Sprintf("split/%v_%v.txt", file.Name(), counter), buffer, 0644)
						if err != nil {
							log.Fatal("File Creation:", err)
						}
					}
					break
				}
				log.Fatal("File Reading:", err)
			}
			if len(buffer) >= size {
				terminate = true
			}
			if terminate && b == ' ' {
				buffer = append(buffer, b)
				err := os.WriteFile(fmt.Sprintf("split/%v_%v.txt", file.Name(), counter), buffer, 0644)
				if err != nil {
					log.Fatal("File Creation:", err)
				}
				counter += 1
				buffer = buffer[:0]
				terminate = false
			} else {
				buffer = append(buffer, b)
			}
		}
		f.Close()
	}
}
