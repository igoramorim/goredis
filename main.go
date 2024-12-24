package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	sv, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	conn, err := sv.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("invalid command:", command)
			return
		}

		handler(args)
	})

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("invalid request: expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("invalid request: expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		// fmt.Printf("command: %+v\n", command)

		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("invalid command:", command)
			writer.Write(Value{
				typ: "string",
				str: "",
			})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		args := value.array[1:]
		result := handler(args)
		writer.Write(result)
	}
}
