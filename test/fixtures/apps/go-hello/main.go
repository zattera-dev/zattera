// go-hello is the E2E fixture app: a minimal HTTP server with a healthcheck.
// The response body includes FIXTURE_MESSAGE so red/green tests can tell
// releases apart by changing one env var.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	msg := os.Getenv("FIXTURE_MESSAGE")
	if msg == "" {
		msg = "Hello from Zattera fixture"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, msg)
	})
	log.Printf("go-hello listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
