import socket
import ssl
import sys
import time

hostname = sys.argv[1]
context = ssl.create_default_context()

with socket.create_connection((hostname, 443)) as sock:
    print("CONNECTED")
    with context.wrap_socket(sock, server_hostname=hostname) as ssock:
        print(f"WRAPPED ({ssock.version()})")
        nuls = b"\x00" * 8
        n = ssock.write(nuls)
        print(f"WROTE {n} NULS")
        print("READING")
        t0 = time.time()
        msg = ssock.read(8000)
        t1 = time.time()
        print(f"RECEIVED {len(msg)} bytes after {t1 -t0:.3f}s: {msg!r}")
        print("CLOSING")

