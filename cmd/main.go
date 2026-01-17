package main

import (
	"github.com/faizm4765/proglog/internal/server"
)

func main() {
	httpServer := server.NewHttpServer(":8080")
	httpServer.ListenAndServe()
}
