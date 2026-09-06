package main

import (
	"log"
	"net/rpc"
)

type Args struct {
	Filename string
}

var serverAddress string

func main() {
	client, err := rpc.DialHTTP("tcp", serverAddress+":8000")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	serverAddress = "127.0.0.1"
	arg := Args{"./sample.txt_1.txt"}
	var reply string

	rpcCall := client.Go("Worker.Map", &arg, &reply, nil)
	replyCall := <-rpcCall.Done
	if err := replyCall.Error; err != nil {
		log.Fatal("rpc:", err)
	}
}
