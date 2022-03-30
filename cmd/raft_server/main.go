package main

import "Laputa/cmd/raft_server/app"

func main() {
	app.New("raft-server").Run()
}
