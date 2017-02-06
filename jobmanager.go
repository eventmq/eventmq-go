package goemq

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/zeromq/goczmq"
	"io"
	"log"
	"reflect"
	"router"
	"time"
)

var debug bool = false

type Request interface{}

type Message struct {
	Callable     string
	Path         string
	Args         string
	Kwargs       string
	Class_kwargs string
}

func jobmanager(addr string) {

	log.Println("Starting jobmanager")
	// Create a dealer socket and connect it to the router.
	dealer, err := goczmq.NewDealer(addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("jobmanager created and connected")

	send_inform(dealer)

	for {
		// Receive a job
		msg, err := dealer.RecvMessage()
		if err != nil {
			log.Fatal(err)
		}

		command := string(msg[2])
		if debug {
			log.Println("Received command: " + command)
		}

		switch command {
		case "ACK":
			go send_worker_heartbeats(dealer)
			send_ready(dealer)
		case "REQUEST":
			on_request(dealer, msg)
		case "HEARTBEAT":
			continue
		default:
			log.Printf("Unknown command type received: %s", command)
		}
	}
}

func on_request(dealer *goczmq.Sock, request [][]byte) {
	msgid := string(request[3])
	msg := string(request[6])
	log.Println(msg)
	// dec := json.NewDecoder(strings.NewReader(msg))

	var f []interface{}
	if err := json.Unmarshal(request[6], &f); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}

	if m, ok := f[1].(map[string]interface{}); ok {
		if callable, ok := m["callable"].(string); ok {
			var t T
			reflect.ValueOf(&t).MethodByName(callable).Call([]reflect.Value{})
		} else {
			log.Fatal("Couldn't read REQUEST message's callable field")
		}
	} else {
		log.Fatal("Got REQUEST in a bad format")
	}

	send_reply(dealer, msgid)
	send_ready(dealer)
}

func send_inform(dealer *goczmq.Sock) bool {

	dealer.SendFrame([]byte(""), goczmq.FlagMore)
	dealer.SendFrame([]byte("eMQP/1.0"), goczmq.FlagMore)
	dealer.SendFrame([]byte("INFORM"), goczmq.FlagMore)
	dealer.SendFrame([]byte("uuid"), goczmq.FlagMore)
	dealer.SendFrame([]byte(""), goczmq.FlagMore)
	dealer.SendFrame([]byte("worker"), goczmq.FlagNone)

	return true
}

func send_reply(dealer *goczmq.Sock, msgid string) {
	dealer.SendFrame([]byte(""), goczmq.FlagMore)
	dealer.SendFrame([]byte("eMQP/1.0"), goczmq.FlagMore)
	dealer.SendFrame([]byte("REPLY"), goczmq.FlagMore)
	dealer.SendFrame([]byte("uuid"), goczmq.FlagMore)
	dealer.SendFrame([]byte("REPLY"), goczmq.FlagMore)
	dealer.SendFrame([]byte(msgid), goczmq.FlagNone)
}

func send_ready(dealer *goczmq.Sock) {
	dealer.SendFrame([]byte(""), goczmq.FlagMore)
	dealer.SendFrame([]byte("eMQP/1.0"), goczmq.FlagMore)
	dealer.SendFrame([]byte("READY"), goczmq.FlagMore)
	dealer.SendFrame([]byte("uuid"), goczmq.FlagNone)
}

func send_worker_heartbeats(dealer *goczmq.Sock) {
	for {
		unix_time := string(time.Now().Unix())
		dealer.SendFrame([]byte(""), goczmq.FlagMore)
		dealer.SendFrame([]byte("eMQP/1.0"), goczmq.FlagMore)
		dealer.SendFrame([]byte("HEARTBEAT"), goczmq.FlagMore)
		dealer.SendFrame([]byte("uuid"), goczmq.FlagMore)
		dealer.SendFrame([]byte(unix_time), goczmq.FlagNone)
		time.Sleep(5 * time.Second)
	}
}

type T struct{}

func main() {

	var router_addr = flag.String("router_addr", "tcp://127.0.0.1:47290", "Address of router to connect to")
	var router = flag.Bool("router", true, "Router mode")
	flag.Parse()

	if *router {
		go inbound(*router_addr, *router_addr)
	} else {
		go jobmanager(*router_addr)
	}

	var input string
	fmt.Scanln(&input)

}

func (t *T) Test_job() {
	log.Println("Hi, universe!")
}
