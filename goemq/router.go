package goemq

import (
	zmq "github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
	"log"
)

type Config struct {
	DISABLE_HEARTBEATS bool
	HEARTBEAT_INTERVAL uint32
}

type EMQMessage struct {
	Frames []string
}

type Worker struct {
	id   string
	name string
}

var queues map[string][]Worker
var workers []Worker

func Inbound(frontend_addr string, backend_addr string) {

	log.Println("start router")
	// Create a router socket and bind it to port 5555.

	backend := zmq.NewRouter(zmtp.NewSecurityNull(), generate_uuid())

	_, err := backend.Bind(backend_addr)
	if err != nil {
		panic(err)
	}

	frontend := zmq.NewRouter(zmtp.NewSecurityNull(), generate_uuid())
	_, frontend_err := frontend.Bind(frontend_addr)
	if frontend_err != nil {
		log.Fatal(frontend_err)
	}

	log.Println("Backend and frontend router socket created and bound")

	for {
		// Receve the message. Here we call RecvMessage, which
		// will return the message as a slice of frames ([][]byte).
		// Since this is a router socket that support async
		// request / reply, the first frame of the message will
		// be the routing frame.
		request, err := backend.RecvMultipart()

		if err == nil {
			log.Printf("backend received '%s' from '%v'", request[3], request[0])

			go process_worker_message(backend, request)
		}

		frontend_request, err := frontend.RecvMultipart()

		if err == nil {
			log.Printf("frontend received '%s' from '%v'", frontend_request[3], frontend_request[0])
		}

		go process_client_message(frontend, frontend_request)
	}
}

// Processes a message incoming on the frontend socket
func process_client_message(backend zmq.Router, request [][]byte) bool {
	command := string(request[3])

	switch command {
	case "INFORM":
		on_inform(backend, request)
	case "DISCONNECT":
		on_disconnect(backend, request)
	case "REQUEST":
		router_on_request(backend, request)
	case "SCHEDULE":
		on_schedule(backend, request)
	case "UNSCHEDULE":
		on_unschedule(backend, request)
	}

	return true
}

// Processes a message incoming on the backend socket
func process_worker_message(backend zmq.Router, request [][]byte) bool {
	command := string(request[3])

	switch command {
	case "INFORM":
		on_inform(backend, request)
	case "DISCONNECT":
		on_disconnect(backend, request)
	case "REQUEST":
		router_on_request(backend, request)
	}
	return true
}

func router_on_request(backend zmq.Router, request [][]byte) {
}

func on_disconnect(backend zmq.Router, request [][]byte) {
}

func on_schedule(backend zmq.Router, request [][]byte) {
}

func on_unschedule(backend zmq.Router, request [][]byte) {
}

func on_inform(backend zmq.Router, request [][]byte) {
	add_worker(string(request[0]), request[1])

	send_ack(backend, request[0], request[4])
}

func send_ack(socket zmq.Router, sender []byte, msgid []byte) {
	log.Println("Sending ACK to " + string(sender))
	msg := [][]byte{
		sender,
		[]byte(""),
		[]byte("eMQP/1.0"),
		[]byte("ACK"),
		[]byte(generate_uuid()),
		msgid,
	}

	socket.SendMultipart(msg)
}

func add_worker(sender string, queues []byte) {

	log.Println("Adding worker...")
	var worker Worker
	worker.id = generate_uuid()
	worker.name = sender
	log.Println(sender)
}

func send_schedluer_heartbeats() {

}

func generate_uuid() string {
	uuid, err := zmq.NewUUID()
	if err != nil {
	}

	return uuid
}
