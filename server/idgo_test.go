package server

import (
	"fmt"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

var wg sync.WaitGroup
var ts *Server

func init() {
	var err error

	// load config
	cfg, err = ParseConfigFile("../config/idgo.toml")
	if err != nil {
		panic(err.Error())
	}

	// new server
	ts, err = NewServer()
	if err != nil {
		panic(err.Error())
	}
}

func forGetId(idGenerator *MySQLIdGenerator) {
	defer wg.Done()
	for i := 0; i < 100; i++ {
		_, err := idGenerator.Next()
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func TestMySQLId1Gen(t *testing.T) {
	idGenerator, err := NewMySQLIdGenerator(ts.db, "idgen_test")
	if err != nil {
		t.Fatal(err.Error())
	}
	err = idGenerator.Reset(1, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	// 10 goroutine
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go forGetId(idGenerator)
	}
	wg.Wait()

	id, err := idGenerator.Next()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(id)
}

func BenchmarkMySQLIdGen(b *testing.B) {
	idGenerator, err := NewMySQLIdGenerator(ts.db, "idgen_bench")
	if err != nil {
		b.Fatal(err.Error())
	}

	err = idGenerator.Reset(1, false)
	if err != nil {
		b.Fatal(err.Error())
	}

	var gid int64

	b.StartTimer()
	fmt.Println("start id: ", b.N)
	// for i := 0; i < 1000; i++ {
	for i := 0; i < b.N; i++ {
		gid, err = idGenerator.Next()
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	b.StopTimer()

	fmt.Println("last Gid: ", gid)
}
