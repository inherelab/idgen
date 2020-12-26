package main

import (
	"github.com/gookit/gcli/v2"
	"github.com/inherelab/genid/cmd"
)

// Local run:
// 	go run ./cmd/genid/main.go --log-level debug
// Bench test:
// 	go test -bench Gen ./mysqlid
func main() {
	app := gcli.NewApp(func(app *gcli.App) {
		app.Name = "ID Generator"
		app.Version = "1.0.1"
		app.Description = "this is Id generator console application"
	})

	app.Add(cmd.HttpServeCommand, cmd.RdsServeCommand)

	app.Run()
}
