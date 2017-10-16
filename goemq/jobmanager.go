package goemq

import (
	"encoding/json"
	zmq "github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
	"io"
	"log"
	"reflect"
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

func Jobmanager(addr string) {

	log.Println("Starting jobmanager")
	// Create a dealer socket and connect it to the router.
	dealer := zmq.NewDealer(zmtp.NewSecurityNull(), generate_uuid())
	err := dealer.Connect(addr)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("jobmanager created and connected")

	send_inform(dealer)

	for {
		// Receive a job
		msg, err := dealer.RecvMultipart()
		if err != nil {
			log.Fatal(err)
		}

		command := string(msg[3])
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

func on_request(dealer zmq.Dealer, request [][]byte) {
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

func send_inform(dealer zmq.Dealer) bool {

	msg := [][]byte{
		[]byte(""),
		[]byte("eMQP/1.0"),
		[]byte("INFORM"),
		[]byte("uuid"),
		[]byte(""),
		[]byte("worker"),
	}

	dealer.SendMultipart(msg)

	return true
}

func send_reply(dealer zmq.Dealer, msgid string) {
	msg := [][]byte{
		[]byte(""),
		[]byte("eMQP/1.0"),
		[]byte("REPLY"),
		[]byte("uuid"),
		[]byte("REPLY"),
		[]byte(msgid),
	}
	dealer.SendMultipart(msg)
}

func send_ready(dealer zmq.Dealer) {

	msg := [][]byte{
		[]byte(""),
		[]byte("eMQP/1.0"),
		[]byte("READY"),
		[]byte("uuid"),
	}

	dealer.SendMultipart(msg)
}

func send_worker_heartbeats(dealer zmq.Dealer) {
	for {
		unix_time := string(time.Now().Unix())
		msg := [][]byte{
			[]byte(""),
			[]byte("eMQP/1.0"),
			[]byte("HEARTBEAT"),
			[]byte("uuid"),
			[]byte(unix_time),
		}
		dealer.SendMultipart(msg)
		time.Sleep(5 * time.Second)
	}
}

type T struct{}

func (t *T) Test_job() {
	log.Println("Hi, universe!")
}
