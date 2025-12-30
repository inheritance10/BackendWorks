package main

import (
	"fmt"
	"net/http"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Worker job started")
	time.sleep(2 * time.Second) //burada cpu / I/O simülasyony yapıyoruz

	fmt.Println("Worker job finished")

	w.Write([]byte("Ok"))
}

funck main() {
	http.HandleFunc("/job", handler)

	fmt.Println("Go Worker running on :5000")

	http.ListenAndServe(":5000, nil")
}