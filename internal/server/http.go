package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// we use http.Server for our server implementation and newHttpServer function to create and configure it.
func NewHttpServer(addr string) *http.Server {
	server := newHTTPServer()
	r := mux.NewRouter()

	// we neeed 2 handlers one for producing and one for consuming log records
	r.HandleFunc("/", server.handleProduce).Methods("POST")
	r.HandleFunc("/", server.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// httpServer struct is needed because the handlers need access to the log and we can achieve that by making log a field of httpServer.
type httpServer struct {
	log *Log
}

// we need to return pointer because httpServer has methods with pointer receivers.
func newHTTPServer() *httpServer {
	server := &httpServer{
		log: NewLog(),
	}

	return server
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset int `json:"offset"`
}

type ConsumeRequest struct {
	Offset int `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

// we have pointer receiver because we are modifying the state of the server by appending to the log.
func (server *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	//  step1: unmarshall request body into the request struct
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// step2: append the record to the log
	off, err := server.log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// step3: marshall the response struct into the response body
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// here we have pointer receiver because we are reading from the log which is part of the server state. if we remove pointer receiver we will have a copy of the server struct and any changes made to the log will not be reflected in the original server struct. SO it means we will not be able to read the records that were appended to the log and we will always have stale data from the point of time the copy was made.
func (server *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var request ConsumeRequest
	// step1: unmarshall request body into the request struct
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// step2: read the record from the log
	record, err := server.log.Read(request.Offset)
	if err == ErrOffsetOutOfRange {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// step3: marshall the response struct into the response body
	response := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
