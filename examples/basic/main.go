package main

import (
	"context"
	"fmt"
	"time"

	"github.com/devflex-pro/chanprobe"
)

func main() {
	ctx := context.Background()

	jobs := chanprobe.New[string]("jobs", 4)
	defer jobs.Close()

	_ = jobs.Send(ctx, "hello")
	_ = jobs.Send(ctx, "world")

	for i := 0; i < 2; i++ {
		job, ok := jobs.Recv(ctx)
		if !ok {
			return
		}
		fmt.Println("processed:", job)
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("%+v\n", jobs.Snapshot())
}
