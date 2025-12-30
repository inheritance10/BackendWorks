package main

import (
	"fmt",
	"net/http"
)

func cpuHeavyTask() int64 {
	var sum int64 = 0
	for i:=int64(0); i<=50000000; i++ {
		sum +=i
	}

	return sum
}

func handler(w http.ResponseWrite, r*http.Request) {
	result :=cpuHeavyTask()
	fmt.Fprintf(w, "CPU result: %d\n", result)
}

func main() {
	http.HandleFunc("/cpu", handler)
	fmt.Println("Go Service running on :4000")
	http.ListenAndServe(":4000", nil)
	
}