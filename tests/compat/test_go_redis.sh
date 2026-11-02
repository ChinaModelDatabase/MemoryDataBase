#!/usr/bin/env bash
set -euo pipefail
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-6379}

if ! command -v go >/dev/null 2>&1; then
  echo "skip: no go"
  exit 0
fi

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/jx-go-smoke.XXXXXX")"
trap 'rm -rf "${WORKDIR}"' EXIT

cat >"${WORKDIR}/go.mod" <<'EOF'
module jxsmoke

go 1.22
EOF

cat >"${WORKDIR}/main.go" <<'EOF'
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	host := os.Getenv("GOHOST")
	port := os.Getenv("GOPORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect failed:", err)
		os.Exit(1)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	cmd := func(args ...string) interface{} {
		if _, err := fmt.Fprintf(rw, "*%d\r\n", len(args)); err != nil {
			panic(err)
		}
		for _, a := range args {
			if _, err := fmt.Fprintf(rw, "$%d\r\n%s\r\n", len(a), a); err != nil {
				panic(err)
			}
		}
		if err := rw.Flush(); err != nil {
			panic(err)
		}
		return readReply(rw.Reader)
	}

	if got := cmd("PING"); got != "PONG" {
		fmt.Fprintln(os.Stderr, "PING failed:", got)
		os.Exit(1)
	}
	if got := cmd("SET", "gok", "gov"); got != "OK" {
		fmt.Fprintln(os.Stderr, "SET failed:", got)
		os.Exit(1)
	}
	if got := cmd("GET", "gok"); got != "gov" {
		fmt.Fprintln(os.Stderr, "GET failed:", got)
		os.Exit(1)
	}
	if got := cmd("DEL", "gok"); got != int64(1) {
		fmt.Fprintln(os.Stderr, "DEL failed:", got)
		os.Exit(1)
	}
	fmt.Println("go-redis smoke OK")
}

func readReply(r *bufio.Reader) interface{} {
	line, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		panic("empty reply")
	}
	rest := line[1:]
	switch line[0] {
	case '+':
		return rest
	case '-':
		panic(rest)
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			panic(err)
		}
		return n
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			panic(err)
		}
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			panic(err)
		}
		return string(buf[:n])
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			panic(err)
		}
		if n < 0 {
			return nil
		}
		out := make([]interface{}, n)
		for i := range out {
			out[i] = readReply(r)
		}
		return out
	default:
		panic("bad reply: " + line)
	}
}
EOF

cd "${WORKDIR}"
GOHOST="${HOST}" GOPORT="${PORT}" go run .
