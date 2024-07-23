
import http.server
import io
import socket
import subprocess
import sys
import threading
import time

OUTPUT = io.StringIO()

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # add a # at the beginning of the line to not mess up Mtest diffs
        OUTPUT.write("#%s - - [%s] %s\n" %
                         (self.address_string(),
                          self.log_date_time_string(),
                          format%args))

    def do_GET(self):
        if self.path == '/42a':
            self.send_response(http.HTTPStatus.OK)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'a' * 42)
        elif self.path == '/40Ka':
            self.send_response(http.HTTPStatus.OK)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'a' * 40 * 1024)
        elif self.path == '/sleep':
            # used to test if subprocess timeout= option works
            time.sleep(60)
            self.send_response(http.HTTPStatus.OK)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"now i'm well rested")
        elif self.path == '/xyzzy':
            msg = 'A hollow voice says "Fool."\n'
            self.send_response(http.HTTPStatus.NOT_FOUND, msg)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(bytes(msg, 'ascii'))
        else:
            self.send_response(http.HTTPStatus.NOT_FOUND)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'NOT FOUND\n')

b = threading.Barrier(2)

def runserver():
    global port
    addr = ('127.0.0.1', 0)
    srv = http.server.HTTPServer(addr, Handler)
    port = srv.server_port
    print(f"Listening on {port}", file=OUTPUT)
    b.wait()
    srv.serve_forever()

# Start the http server
port = 0
t = threading.Thread(target=lambda: runserver(), daemon=True)
t.start()
# and wait for it to fill in the port
b.wait()

url = f'http://localhost:{port}'

def streamcat(suffix):
    u = url + suffix
    cmd = ['streamcat', 'read', u, 'urlstream']
    print(f'FETCHING {suffix}', file=OUTPUT)
    PIPE = subprocess.PIPE
    p = subprocess.run(cmd, check=False, stdout=PIPE, stderr=PIPE, timeout=10)
    print(f' yielded return code {p.returncode}', file=OUTPUT)
    print(f' wrote {len(p.stdout)} bytes to stdout', file=OUTPUT)
    if p.stderr:
        print(f' stderr={p.stderr!r}', file=OUTPUT)
    return (p.returncode, p.stdout, p.stderr)

def run_tests():
    (code, out, err) = streamcat('/42a')
    assert code == 0
    assert out == 42 * b'a'
    assert err == b''

    (code, out, err) = streamcat('/40Ka')
    assert code == 0
    assert out == 40 * 1024 * b'a'
    assert err == b''

    # Are we able to time out?
    # (code, out, err) = streamcat('/sleep')

    (code, out, err) = streamcat('/xyzzy')
    assert code != 0
    assert b'404' in err

try:
    run_tests()
except Exception as e:
    output = OUTPUT.getvalue()
    if output:
        print(output, file=sys.stderr)
    raise e
