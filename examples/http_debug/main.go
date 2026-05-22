package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/devflex-pro/chanprobe"
)

func main() {
	ctx := context.Background()
	jobs := chanprobe.New[string]("http_debug_jobs", 16)
	defer jobs.Close()

	chanprobe.PublishExpvar("chanprobe", nil)

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; ; i++ {
			<-ticker.C
			if err := jobs.Send(ctx, fmt.Sprintf("job-%d", i)); err != nil {
				return
			}
		}
	}()

	go func() {
		for {
			job, ok := jobs.Recv(ctx)
			if !ok {
				return
			}
			log.Println("processed", job)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	log.Println("listening on http://localhost:8080/debug/vars")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
