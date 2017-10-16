package main

import (
	"flag"
	"fmt"
	goemq "github.com/sideshowdave7/eventmqgo/goemq"
)

func main() {

	var router_addr = flag.String("router_addr", "tcp://127.0.0.1:47290", "Address of router to connect to")
	flag.Parse()

	go goemq.Jobmanager(*router_addr)

	var input string
	fmt.Scanln(&input)

}
