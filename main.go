package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/artshmelev/simple-db/db"
)

func main() {
	db, err := db.NewDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		k := q.Get("k")
		v, err := db.Get(k)
		if err != nil {
			fmt.Println("get error", err)
			return
		}
		_, err = w.Write([]byte(v))
		if err != nil {
			fmt.Println(err)
		}
	})
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		k := q.Get("k")
		v := q.Get("v")
		if err := db.Set(k, v); err != nil {
			fmt.Println("set error", err)
		}
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
