package main

import (
	"fmt"
	"net/http"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ping" {
		time.Sleep(10 * time.Millisecond) // I/O simülasyonu
		w.Write([]byte("pong"))
	}
}

func main() {
	http.HandleFunc("/ping", handler)
	fmt.Println("Go server running on :3001")
	http.ListenAndServe(":3001", nil)
}
