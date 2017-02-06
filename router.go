package goemq

import (
	"github.com/zeromq/goczmq"
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

func inbound(frontend_addr string, backend_addr string) {

	log.Println("start router")
	// Create a router socket and bind it to port 5555.
	backend, err := goczmq.NewRouter(backend_addr)
	if err != nil {
		log.Fatal(err)
	}
	frontend, err := goczmq.NewRouter(frontend_addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Backend and frontend router socket created and bound")

	for {
		// Receve the message. Here we call RecvMessage, which
		// will return the message as a slice of frames ([][]byte).
		// Since this is a router socket that support async
		// request / reply, the first frame of the message will
		// be the routing frame.
		request, err := backend.RecvMessage()

		if err == nil {
			log.Printf("backend received '%s' from '%v'", request[3], request[0])

			go process_worker_message(backend, request)
		}

		request, err := frontend.RecvMessage()

		if err == nil {
			log.Printf("frontend received '%s' from '%v'", request[3], request[0])
		}

		go process_client_message(frontend, request)
	}
}

// Processes a message incoming on the frontend socket
func process_client_message(backend *goczmq.Sock, request [][]byte) bool {
	command := string(request[3])

	switch command {
	case "INFORM":
		on_inform(backend, request)
	case "DISCONNECT":
		on_disconnect(backend, request)
	case "REQUEST":
		on_request(backend, request)
	case "SCHEDULE":
		on_schedule(backend, request)
	case "UNSCHEDULE":
		on_unschedule(backend, request)
	}

	return true
}

// Processes a message incoming on the backend socket
func process_worker_message(backend *goczmq.Sock, request [][]byte) bool {
	command := string(request[3])

	switch command {
	case "INFORM":
		on_inform(backend, request)
	case "DISCONNECT":
		on_disconnect(backend, request)
	case "REQUEST":
		on_request(backend, request)
	}
	return true
}

func on_request(backend *goczmq.Sock, request [][]byte) {
}

func on_disconnect(backend *goczmq.Sock, request [][]byte) {
}

func on_schedule(backend *goczmq.Sock, request [][]byte) {
}

func on_unschedule(backend *goczmq.Sock, request [][]byte) {
}

func on_inform(backend *goczmq.Sock, request [][]byte) {
	add_worker(request[0], request[1])

	send_ack(backend, request[0])
}

func send_ack(socket *goczmq.Sock, sender string) {
	log.Println("Sending ACK to " + sender)
	socket.SendFrame(sender, goczmq.FlagMore)
	socket.SendFrame([]byte(""), goczmq.FlagMore)
	socket.SendFrame([]byte("eMQP/1.0"), goczmq.FlagMore)
	socket.SendFrame([]byte("ACK"), goczmq.FlagMore)
	socket.SendFrame([]byte("uuid"), goczmq.FlagMore)
	socket.SendFrame(request[4], goczmq.FlagNone)
}

func add_worker(sender *string, queues []string) {

	log.Println("Adding worker...")
	var worker Worker
	worker.id = '1'
	worker.name = "Interesting Name"
	log.Println(sender)
}

func send_schedluer_heartbeats() {

}
