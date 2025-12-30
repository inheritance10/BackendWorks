package main

import (
	"fmt"
	"time"
)

func main() {
	start:= time.Now();

	var sum int64 = 0;

	for i:=int64(1); i<=100000000; i++ {
		sum += i;
	}

	elapsed := time.Since(start);

	fmt.Println("Sum:", sum)
	fmt.Println("Time:", elapsed)

}