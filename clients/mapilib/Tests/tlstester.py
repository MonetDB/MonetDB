#!/usr/bin/env python3

from argparse import ArgumentParser
from datetime import datetime, timedelta
import hashlib
import http.server
import io
import logging
import os
import socket
import socketserver
import ssl
from ssl import AlertDescription, SSLContext, SSLEOFError, SSLError
import struct
import sys
import tempfile
from threading import Thread
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cryptography import x509
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa

VERSION = "0.3.1"

DESCRIPTION = f"tlstester.py version {VERSION}: a utility to help test TLS MAPI client implementations."

log = logging.getLogger("tlstester")

argparser = ArgumentParser("tlstester", description=DESCRIPTION)
argparser.add_argument(
    "-p",
    "--base-port",
    type=int,
    help="base port on which this utility is reachable",
)
argparser.add_argument(
    "-w",
    "--write",
    type=str,
    metavar="DIR",
    help="Write generated keys and certs to this directory",
)
argparser.add_argument(
    "-l",
    "--listen-addr",
    type=str,
    default="localhost",
    help="interface to listen on, default=localhost",
)
argparser.add_argument(
    "-n",
    "--hostname",
    action="append",
    type=str,
    default=[],
    help="server name to sign certificates for, can be repeated, default=localhost.localdomain",
)
argparser.add_argument(
    "--sequential",
    action="store_true",
    help="allocate ports sequentially after BASE_PORT, instead of whatever the OS decides",
)
argparser.add_argument(
    "-a",
    "--assign",
    action="append",
    metavar="NAME=PORTNUM",
    default=[],
    help="force port assignment",
)
argparser.add_argument(
    "-f",
    "--forward",
    metavar="LOCALPORT:FORWARDHOST:FORWARDPORT",
    type=str,
    help="forward decrypted traffic somewhere else",
)
argparser.add_argument(
    "-v", "--verbose", action="store_true", help="Log more information"
)


class Certs:
    hostnames: str
    _files: Dict[str, bytes]
    _keys: Dict[x509.Name, rsa.RSAPrivateKey]
    _certs: Dict[x509.Name, x509.Certificate]
    _parents: Dict[x509.Name, x509.Name]

    def __init__(self, hostnames: List[str]):
        self.hostnames = hostnames
        self._files = {}
        self._keys = {}
        self._certs = {}
        self._parents = {}
        self.gen_keys()

    def get_file(self, name):
        return self._files.get(name)

    def all(self) -> Dict[str, str]:
        return self._files.copy()

    def gen_keys(self):
        ca1 = self.gen_ca("ca1")
        self.gen_server("server1", ca1)
        self.gen_server("server1x", ca1, not_before=-15, not_after=-1)
        ca2 = self.gen_ca("ca2")
        self.gen_server("server2", ca2)
        self.gen_server("client2", ca2, keycrt=True)
        ca3 = self.gen_ca("ca3")
        self.gen_server("server3", ca3)

    def gen_ca(self, name: str):
        ca_name = x509.Name(
            [
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, f"Org {name}"),
                x509.NameAttribute(
                    x509.NameOID.COMMON_NAME, f"The Certificate Authority"
                ),
            ]
        )
        critical_ca_extensions = [x509.BasicConstraints(ca=True, path_length=1)]
        self.gen_key(name, ca_name, critical_extensions=critical_ca_extensions)

        return ca_name

    def gen_server(
        self, name: str, ca_name: x509.Name, not_before=0, not_after=14, keycrt=False
    ):
        assert self.hostnames
        server_name = x509.Name(
            [
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, f"Org {name}"),
                x509.NameAttribute(x509.NameOID.COMMON_NAME, self.hostnames[0]),
            ]
        )
        noncritical_server_extensions = [
            x509.SubjectAlternativeName([x509.DNSName(n) for n in self.hostnames])
        ]
        self.gen_key(
            name=name,
            subject_name=server_name,
            parent_name=ca_name,
            not_before=not_before,
            not_after=not_after,
            noncritical_extensions=noncritical_server_extensions,
            keycrt=keycrt,
        )

    def gen_key(
        self,
        name: str,
        subject_name: x509.Name,
        parent_name: Optional[x509.Name] = None,
        not_before=0,
        not_after=14,
        critical_extensions: List[x509.ExtensionType] = [],
        noncritical_extensions: List[x509.ExtensionType] = [],
        keycrt=False,
    ):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

        if parent_name:
            issuer_name = parent_name
            issuer_key = self._keys[parent_name]
        else:
            issuer_name = subject_name
            issuer_key = key

        now = datetime.utcnow()
        builder = (
            x509.CertificateBuilder()
            .issuer_name(issuer_name)
            .subject_name(subject_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now + timedelta(not_before))
            .not_valid_after(now + timedelta(not_after))
        )
        for ext in critical_extensions:
            builder = builder.add_extension(ext, critical=True)
        for ext in noncritical_extensions:
            builder = builder.add_extension(ext, critical=False)
        cert = builder.sign(issuer_key, hashes.SHA256())

        self._keys[subject_name] = key
        self._certs[subject_name] = cert
        self._parents[subject_name] = parent_name

        pem_key = key.private_bytes(
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encoding=serialization.Encoding.PEM,
            encryption_algorithm=serialization.NoEncryption(),
        )
        self.insert_file(f"{name}.key", pem_key)

        n = subject_name
        pem_crt = b""
        while n:
            c = self._certs[n]
            pem_crt += c.public_bytes(serialization.Encoding.PEM)
            n = self._parents.get(n)
        self.insert_file(f"{name}.crt", pem_crt)

        der_crt = self._certs[subject_name].public_bytes(serialization.Encoding.DER)
        self.insert_file(f"{name}.der", der_crt)

        if keycrt:
            pem_keycrt = pem_key + pem_crt
            self.insert_file(f"{name}.keycrt", pem_keycrt)

    def insert_file(self, name, content):
        assert isinstance(content, bytes)
        assert name not in self._files
        self._files[name] = content


class TLSTester:
    certs: Certs
    hostnames: List[str]
    listen_addr: str
    forward_to: Optional[Tuple[str, int]] = None
    preassigned: Dict[str, int]
    portmap: Dict[str, int]
    next_port: int
    workers: List[Callable[[], None]]

    def __init__(
        self,
        certs: Certs,
        listen_addr: str,
        preassigned,
        sequential,
        forward_host=None,
        forward_port=None,
        hostnames=None,
    ):
        self.certs = certs
        self.hostnames = hostnames or []
        self.listen_addr = listen_addr
        if forward_host or forward_port:
            self.forward_to = (forward_host, forward_port)
        self.preassigned = preassigned
        self.portmap = dict()
        if "base" in preassigned and sequential:
            self.next_port = preassigned["base"] + 1
        else:
            self.next_port = 0
        self.workers = []

        self.spawn_listeners(only_preassigned=True)
        self.spawn_listeners(only_preassigned=False)

    def base_port(self) -> int:
        return self.get_port("base")

    def get_port(self, name) -> int:
        return self.portmap[name]

    def spawn_listeners(self, only_preassigned: bool):
        self.spawn_http("base", only_preassigned)
        self.spawn_mapi("server1", only_preassigned, self.ssl_context("server1"))
        self.spawn_mapi("server2", only_preassigned, self.ssl_context("server2"))
        self.spawn_mapi("server3", only_preassigned, self.ssl_context("server3"))
        self.spawn_mapi("plain", only_preassigned, None)
        self.spawn_mapi("expiredcert", only_preassigned, self.ssl_context("server1x"))
        self.spawn_mapi(
            "tls12",
            only_preassigned,
            self.ssl_context("server1", allow_tlsv12=True),
        )
        self.spawn_mapi(
            "clientauth",
            only_preassigned,
            self.ssl_context("server1", client_cert="ca2"),
        )
        if self.hostnames:
            self.spawn_mapi(
                "sni", only_preassigned, self.ssl_context("server1", hostnames=self.hostnames)
            )
        self.spawn_mapi(
            "alpn_mapi9",
            only_preassigned,
            self.ssl_context("server1"),
            check_alpn=["mapi/9"]
        )
        self.spawn_mapi("redirect", only_preassigned, self.ssl_context("server1"), redirect_to="server2")
        if self.forward_to:
            self.spawn_forward("forward", self.ssl_context("server3"))

    def ssl_context(
        self, cert_name: str, allow_tlsv12=False, client_cert=None, hostnames=[]
    ):
        context = make_context(allow_tlsv12)
        context.set_alpn_protocols(["mapi/9"])

        def sni_callback(sock, server_name, ctx):
            if hostnames:
                if server_name is None:
                    log.info(f"        client sent no server name")
                    return AlertDescription.ALERT_DESCRIPTION_UNRECOGNIZED_NAME
                elif server_name not in hostnames:
                    log.info(f"        client sent invalid server name '{server_name}'")
                    return AlertDescription.ALERT_DESCRIPTION_UNRECOGNIZED_NAME
                else:
                    log.debug(f"        client sent server name '{server_name}'")
            return None
        try:
            context.sni_callback = sni_callback
        except AttributeError:
            context.set_servername_callback(sni_callback)

        # Turns out the ssl API forces us to write the certs to file. Yuk!
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            f.write(self.certs.get_file(cert_name + ".key"))
            f.write(self.certs.get_file(cert_name + ".crt"))
            f.flush()
            context.load_cert_chain(f.name)

        if client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
            cert_bytes = self.certs.get_file(client_cert + ".crt")
            cert_str = str(cert_bytes, "utf-8")
            context.load_verify_locations(cadata=cert_str)

        return context

    def serve_forever(self):
        threads = []
        for worker in self.workers:
            t = Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def spawn_http(self, name: str, only_preassigned: bool):
        if only_preassigned and name not in self.preassigned:
            return
        if name in self.portmap:
            return
        port = self.preassigned.get(name, 0)
        handler = lambda req, addr, server: WebHandler(
            req, addr, server, self.certs, self.portmap
        )
        server = http.server.HTTPServer((self.listen_addr, port), handler)
        port = server.server_address[1]
        log.debug(f"Bound port {name} to {port}")
        self.portmap[name] = port
        self.workers.append(server.serve_forever)

    def spawn_mapi(self, name: str, only_preassigned, ctx: SSLContext, check_alpn=None, redirect_to=None):
        if only_preassigned and name not in self.preassigned:
            return
        if name in self.portmap:
            return
        port = self.allocate_port(name)
        handler = lambda req, addr, server: MapiHandler(req, addr, server, self, name, ctx, check_alpn, redirect_to)
        server = MyTCPServer((self.listen_addr, port), handler)
        port = server.server_address[1]
        log.debug(f"Bound port {name} to {port}")
        self.portmap[name] = port
        self.workers.append(server.serve_forever)

    def spawn_forward(self, name, ctx: SSLContext):
        if name in self.portmap:
            return
        local_port = self.preassigned[name]
        handler = lambda req, addr, server: ForwardHandler(
            req, addr, server, name, ctx, self.forward_to
        )
        server = MyTCPServer((self.listen_addr, local_port), handler)
        port = server.server_address[1]
        log.debug(f"Bound port {name} to {port}")
        self.portmap[name] = port
        self.workers.append(server.serve_forever)

    def allocate_port(self, name):
        if name in self.preassigned:
            port = self.preassigned[name]
        elif self.next_port > 0:
            while self.next_port in self.portmap.values():
                self.next_port += 1
            port = self.next_port
        else:
            port = 0
        return port


def make_context(allowtlsv12 = False):
    # Older versions of the ssl module don't have ssl.TLSVersion, so
    # we have four combinations.

    protocol = ssl.PROTOCOL_TLS_SERVER
    opts = ssl.OP_NO_SSLv2
    opts |= ssl.OP_NO_SSLv3
    opts |= ssl.OP_NO_TLSv1
    if allowtlsv12:
        opts |= ssl.OP_NO_TLSv1_3
    else:
        opts |= ssl.OP_NO_TLSv1_2

    context = SSLContext(protocol)
    context.options = opts

    if hasattr(context, 'minimum_version'):
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        if allowtlsv12:
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        else:
            context.minimum_version = ssl.TLSVersion.TLSv1_3

    return context

class WebHandler(http.server.BaseHTTPRequestHandler):
    certs: Certs
    portmap: Dict[str, int]

    def __init__(self, req, addr, server, certs: Certs, portmap: Dict[str, int]):
        self.certs = certs
        self.portmap = portmap
        super().__init__(req, addr, server)

    def do_GET(self):
        idx = self.path.find("?")
        path = self.path[:idx] if idx > 0 else self.path
        if path == "/":
            return self.do_root()
        content = self.certs.get_file(path[1:])
        if content:
            return self.do_content(content)
        self.send_error(http.HTTPStatus.NOT_FOUND)

    def do_root(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        w = io.TextIOWrapper(
            self.wfile,
            encoding="ascii",
        )
        for name, port in self.portmap.items():
            print(f"{name}:{port}", file=w)
        w.flush()
        w.detach()

    def do_content(self, content: bytes):
        try:
            str(content, encoding="ascii")
            content_type = "text/plain; charset=utf-8"
        except UnicodeDecodeError:
            content_type = "application/binary"

        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(content)


class MyTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    pass


class MapiHandler(socketserver.BaseRequestHandler):
    tlstester: TLSTester
    name: str
    context: SSLContext
    conn: ssl.SSLSocket
    check_alpn: Optional[List[str]]
    redirect: Optional[str]

    CHALLENGE = b"s7NzFDHo0UdlE:merovingian:9:RIPEMD160,SHA512,SHA384,SHA256,SHA224,SHA1:LIT:SHA512:"
    ERRORMESSAGE = "!Sorry, this is not a real MonetDB instance"

    def __init__(self, req, addr, server, tlstester, name, context, check_alpn, redirect_to):
        self.tlstester = tlstester
        self.name = name
        self.context = context
        self.check_alpn = check_alpn
        self.redirect = redirect_to
        super().__init__(req, addr, server)

    def handle(self):
        log.debug(f"port '{self.name}': new connection")
        if self.context:
            log.debug(f"port '{self.name}': trying to set up TLS")
            try:
                self.conn = self.context.wrap_socket(self.request, server_side=True)
                log.info(f"port '{self.name}': TLS handshake succeeded: {self.conn.version()}")
            except SSLError as e:
                log.info(f"port '{self.name}': TLS handshake failed: {e}")
                return
            if self.check_alpn:
                alpn = self.conn.selected_alpn_protocol()
                if alpn is None:
                    log.info(f"port '{self.name}': Abort connection because ALPN negotiation failed")
                    return
                elif alpn not in self.check_alpn:
                    log.info(f"port '{self.name}': Abort connection because selected ALPN protocol '{alpn}' is not in {self.check_alpn}")
                    return
                else:
                    log.debug(f"port '{self.name}': selected suitable ALPN protocol '{alpn}'")
        else:
            self.conn = self.request
            log.info(f"port '{self.name}' no TLS handshake necessary")

        try:
            self.send_message(self.CHALLENGE)
            log.debug(f"port '{self.name}': sent challenge, awaiting response")
            if self.recv_message():
                if self.redirect:
                    host = self.tlstester.hostnames[0]
                    port = self.tlstester.portmap[self.redirect]
                    cert = self.tlstester.certs.get_file(f"{self.redirect}.der")
                    algo = 'sha256'
                    digest = hashlib.new(algo, cert).hexdigest()
                    fingerprint = "{" + algo + "}" + digest
                    msg = f"^monetdbs://{host}:{port}?certhash={fingerprint}\n"
                    self.send_message(bytes(msg, 'ascii'))
                    log.debug(
                        f"port '{self.name}': sent redirect, sent closing message"
                    )
                else:
                    message = f"{self.ERRORMESSAGE} ({self.name})"
                    self.send_message(bytes(message, "utf-8"))
                    log.debug(
                        f"port '{self.name}': received response, sent closing message"
                    )

        except OSError as e:
            log.info(f"port '{self.name}': error {e}")

    def send_message(self, msg: bytes):
        n = len(msg)
        head = struct.pack("<h", 2 * n + 1)
        self.conn.sendall(head + msg)

    def recv_message(self):
        nread = 0
        while True:
            head = self.recv_bytes(2)
            nread += len(head)
            if len(head) < 2:
                break
            n = struct.unpack("<h", head)[0]
            size = n // 2
            last = (n & 1) > 0
            if size > 0:
                body = self.recv_bytes(size)
                nread += len(body)
                if len(body) < size:
                    break
            if last:
                return True

        log.info(f"port '{self.name}': incomplete message, EOF after {nread} bytes")
        return False

    def recv_bytes(self, size):
        """Read 'size' bytes. Only return fewer if EOF"""
        buf = b""
        while len(buf) < size:
            remaining = size - len(buf)
            more = self.conn.recv(remaining)
            if more == b"":
                return buf
            else:
                buf += more
        return buf


class ForwardHandler(socketserver.BaseRequestHandler):
    name: str
    forward: Tuple[str, int]
    context: SSLContext
    conn: ssl.SSLSocket

    def __init__(self, req, addr, server, name, context, forward):
        self.name = name
        self.forward = forward
        self.context = context
        self.stopping = False
        super().__init__(req, addr, server)

    def handle(self):
        log.debug(f"port '{self.name}': new connection")
        log.debug(f"ACTIVE {[t.name for t in threading.enumerate()]}")
        assert self.context
        if self.context:
            log.debug(f"port '{self.name}': trying to set up TLS")
            try:
                self.conn = self.context.wrap_socket(self.request, server_side=True)
                log.info(f"port '{self.name}': TLS handshake succeeded")
            except SSLError as e:
                log.info(f"port '{self.name}': TLS handshake failed: {e}")
                return
        else:
            self.conn = self.request
            log.info(f"port '{self.name}' no TLS handshake necessary")

        try:
            log.debug(f"attempting to connect to {self.forward}")
            upstream = socket.create_connection(self.forward)
            log.debug(f"connection established")
        except OSError as e:
            log.error("Could not open connection to {self.forward}: {e}")
            return

        Thread(
            target=lambda: self.move("upstream", read_from=self.conn, write_to=upstream)
        ).start()
        self.move("downstream", read_from=upstream, write_to=self.conn)

    def move(self, direction, read_from, write_to):
        try:
            while True:
                data = None
                try:
                    data = read_from.recv(8192)
                    if not data:
                        break
                    write_to.sendall(data)
                except BrokenPipeError:
                    break
                except SSLEOFError:
                    break
                except OSError as e:
                    log.error(f"encountered error {direction}: {e!r}")
                    break
            try:
                write_to.shutdown(socket.SHUT_WR)
            except OSError as e:
                pass
        finally:
            log.debug(f"Done forwarding data {direction}")

    def send_message(self, msg: bytes):
        n = len(msg)
        head = struct.pack("<h", 2 * n + 1)
        self.conn.sendall(head + msg)

    def recv_message(self):
        nread = 0
        while True:
            head = self.recv_bytes(2)
            nread += len(head)
            if len(head) < 2:
                break
            n = struct.unpack("<h", head)[0]
            size = n // 2
            last = (n & 1) > 0
            if size > 0:
                body = self.recv_bytes(size)
                nread += len(body)
                if len(body) < size:
                    break
            if last:
                return True

        log.info(f"port '{self.name}': incomplete message, EOF after {nread} bytes")
        return False

    def recv_bytes(self, size):
        """Read 'size' bytes. Only return fewer if EOF"""
        buf = b""
        while len(buf) < size:
            remaining = size - len(buf)
            more = self.conn.recv(remaining)
            if more == b"":
                return buf
            else:
                buf += more
        return buf


def main(args):
    if args.base_port is None and not args.write and args.forward is None:
        sys.exit(
            "Please specify at least one of -p --base-port, -w --write or -f --forward"
        )
    if args.base_port == 0 and args.sequential:
        sys.exit("--sequential requires a nonzero base port")

    hostnames = args.hostname or ["localhost.localdomain"]
    log.debug(f"Creating certs for {hostnames}")
    certs = Certs(hostnames)
    if args.write:
        dir = args.write
        try:
            os.mkdir(dir)
        except FileExistsError:
            pass
        count = 0
        for name, content in certs.all().items():
            with open(os.path.join(dir, name), "wb") as a:
                a.write(content)
                count += 1
        log.info(f"Wrote {count} files to {dir!r}")

    preassigned = dict()

    for a in args.assign:
        name, num = a.split("=", 1)
        if name == "base":
            sys.exit("use -p --base-port to set the base port")
        preassigned[name] = int(num)

    if args.forward:
        l, h, p = args.forward.split(":", 2)
        preassigned["forward"] = int(l)
        forward_remote_host = h
        forward_remote_port = int(p)
        if args.base_port is None:
            # otherwise listeners won't start
            args.base_port = 0
        elif args.base_port == preassigned["forward"]:
            sys.exit("base port and forward port cannot be the same")
    else:
        forward_remote_host = None
        forward_remote_port = None

    if args.base_port is None:
        return 0
    else:
        preassigned["base"] = args.base_port

    server = TLSTester(
        hostnames=hostnames,
        certs=certs,
        preassigned=preassigned,
        listen_addr=args.listen_addr,
        sequential=args.sequential,
        forward_host=forward_remote_host,
        forward_port=forward_remote_port,
    )

    log.info(f"Serving requests on base port {server.base_port()}")
    server.serve_forever()


if __name__ == "__main__":
    args = argparser.parse_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)
    sys.exit(main(args) or 0)
