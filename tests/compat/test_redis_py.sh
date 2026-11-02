#!/usr/bin/env bash
set -euo pipefail
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-6379}
export HOST PORT
python3 - <<'PY'
import os
import socket

HOST = os.environ["HOST"]
PORT = int(os.environ["PORT"])

s = socket.create_connection((HOST, PORT), timeout=2)
s.settimeout(2)


def _readline():
    buf = b""
    while not buf.endswith(b"\r\n"):
        chunk = s.recv(1)
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf[:-2]


def read_reply():
    line = _readline()
    if not line:
        raise RuntimeError("empty reply")
    kind, rest = line[:1], line[1:]
    if kind == b"+":
        return rest
    if kind == b"-":
        raise RuntimeError(rest.decode("utf-8", "replace"))
    if kind == b":":
        return int(rest)
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        data = b""
        while len(data) < n + 2:
            chunk = s.recv(n + 2 - len(data))
            if not chunk:
                raise EOFError("connection closed mid bulk")
            data += chunk
        return data[:-2]
    if kind == b"*":
        n = int(rest)
        if n < 0:
            return None
        return [read_reply() for _ in range(n)]
    raise RuntimeError(f"bad reply {line!r}")


def cmd(*args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(out)
    return read_reply()


assert cmd("PING") == b"PONG"
assert cmd("SET", "pyk", "pyv") == b"OK"
assert cmd("GET", "pyk") == b"pyv"
assert cmd("DEL", "pyk") == 1

assert cmd("SET", "pyttl", "v") == b"OK"
assert cmd("EXPIRE", "pyttl", 100) == 1
ttl = cmd("TTL", "pyttl")
assert isinstance(ttl, int) and 0 < ttl <= 100, f"TTL={ttl!r}"
assert cmd("DEL", "pyttl") == 1

assert cmd("HSET", "pyhash", "f1", "v1") == 1
assert cmd("HGET", "pyhash", "f1") == b"v1"

assert cmd("RPUSH", "pylist", "a", "b") == 2
assert cmd("LRANGE", "pylist", 0, -1) == [b"a", b"b"]

assert cmd("SADD", "pyset", "m1", "m2") == 2
assert cmd("SISMEMBER", "pyset", "m1") == 1
assert cmd("SISMEMBER", "pyset", "m3") == 0

assert cmd("ZADD", "pyz", 1, "one") == 1
assert cmd("ZADD", "pyz", 1.5, "two") == 1
assert cmd("ZSCORE", "pyz", "one") == b"1"
assert cmd("ZSCORE", "pyz", "two") == b"1.5"

print("redis-py smoke OK")
PY
