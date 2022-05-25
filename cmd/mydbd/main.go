package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"

	"log"

	"github.com/ryicoh/mydb"
)

var (
	dbpath string = "./my.db"
	port   int    = 9888
)

var (
	responseOK    = []byte("+OK\r\n")
	responseError = []byte("-Error message\r\n")
)

func main() {
	flag.StringVar(&dbpath, "path", dbpath, fmt.Sprintf("default: %s", dbpath))
	flag.IntVar(&port, "port", port, fmt.Sprintf("default: %d", port))
	flag.Parse()

	err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func run() error {
	db, err := mydb.New(dbpath)
	if err != nil {
		return err
	}
	defer db.Close()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		requestID := fmt.Sprintf("%d: ", rand.Int31())
		infolog := log.New(os.Stdout, "INFO  ", log.Ldate|log.Ltime|log.Lshortfile|log.LUTC|log.Lmsgprefix)
		infolog.SetPrefix(requestID)
		errorlog := log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Llongfile|log.LUTC|log.Lmsgprefix)
		errorlog.SetPrefix(requestID)

		conn, err := listener.Accept()
		if err != nil {
			errorlog.Println(err)
			continue
		}

		go func() {
			conn := conn

			if err := handleRequest(conn, infolog, db); err != nil {
				errorlog.Println(err)
			}
		}()
	}
}

func handleRequest(conn net.Conn, logger *log.Logger, db *mydb.DB) error {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(line, []byte("*")) {
			return errors.New("unknown request")
		}

		size, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return err
		}

		cmd := make([][]byte, size)
		for i := 0; i < size*2; i++ {
			d, _, err := reader.ReadLine()
			if err != nil {
				return err
			}
			if i%2 == 0 {
				continue
			}
			cmd[i/2] = d
		}

		for _, m := range cmd {
			logger.Println("<-", string(m))
		}

		var res [][]byte
		switch {
		case bytes.EqualFold(cmd[0], []byte("SET")):
			if len(cmd) < 3 {
				res = append(res, []byte("-Missing arguments"))
			} else {
				if err := db.Put(cmd[1], cmd[2]); err != nil {
					return err
				}
				res = append(res, []byte("+OK"))
			}
		case bytes.EqualFold(cmd[0], []byte("GET")):
			value, err := db.Get(cmd[1])
			if err != nil {
				if errors.Is(err, mydb.ErrKeyNotFound) {
					res = append(res, []byte("-Key not found"))
				} else {
					return err
				}
			} else {
				res = append(res, append([]byte("+"), value...))
			}
		default:
			res = append(res, []byte(fmt.Sprintf("-unsupport command `%s`", string(cmd[0]))))
		}

		for _, r := range res {
			logger.Println("->", string(r))
		}

		if _, err := conn.Write(append(bytes.Join(res, []byte("\r\n")), []byte("\r\n")...)); err != nil {
			return err
		}
	}
}
