package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeWaitsForActiveHTTPHandler(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})}
	listening := make(chan net.Addr, 1)
	server.BaseContext = func(listener net.Listener) context.Context {
		listening <- listener.Addr()
		return context.Background()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- serve(ctx, server) }()
	var addr net.Addr
	select {
	case addr = <-listening:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not listen")
	}
	response := make(chan error, 1)
	go func() {
		res, err := http.Get("http://" + addr.String())
		if err == nil {
			res.Body.Close()
		}
		response <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-finished:
		t.Fatalf("shutdown returned before handler: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}

func TestServeReturnsListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := serve(t.Context(), &http.Server{Addr: listener.Addr().String()}); err == nil {
		t.Fatal("expected address in use")
	}
}
