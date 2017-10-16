package main

import (
	"flag"
	"fmt"
	goemq "github.com/sideshowdave7/eventmqgo/goemq"
)

func main() {

	var backend_addr = flag.String("backend_addr", "tcp://127.0.0.1:47290", "Address of router to connect to")
	var frontend_addr = flag.String("frontend_addr", "tcp://127.0.0.1:47291", "Address of router to connect to")

	flag.Parse()

	go goemq.Inbound(*backend_addr, *frontend_addr)

	var input string
	fmt.Scanln(&input)

}
