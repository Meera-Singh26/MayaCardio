"""HTTP front-end of the cloud service (stdlib only, keep-alive, optional API key)."""
from __future__ import annotations

import hmac
import json
import logging
import re
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ..protocol import decode_body
from .service import CloudService

log = logging.getLogger("cloud")


def make_handler(svc: CloudService, api_key: str):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "mayacardio-cloud/1.0"

        def log_message(self, fmt, *a):
            log.debug(fmt, *a)

        def _send(self, code, obj):
            raw = json.dumps(obj, separators=(",", ":")).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _auth(self) -> bool:
            if not api_key:
                return True
            if hmac.compare_digest(self.headers.get("X-API-Key", ""), api_key):
                return True
            self._send(401, {"error": "bad api key"})
            return False

        def _stream(self, lines):
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            buf, size = [], 0
            def flush():
                data = ("\n".join(buf) + "\n").encode()
                self.wfile.write(b"%x\r\n" % len(data) + data + b"\r\n")
            for ln in lines:
                buf.append(ln)
                size += len(ln)
                if size > 64000:
                    flush()
                    buf, size = [], 0
            if buf:
                flush()
            self.wfile.write(b"0\r\n\r\n")

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            try:
                if u.path == "/v1/health":
                    return self._send(200, {"ok": True})
                if not self._auth():
                    return
                if u.path == "/v1/sessions":
                    return self._send(200, {"sessions": svc.store.sessions()})
                m = re.fullmatch(r"/v1/sessions/([^/]+)/(ranges|status|export|results)", u.path)
                if not m:
                    return self._send(404, {"error": "not found"})
                sid, what = m.groups()
                if what == "ranges":
                    return self._send(200, svc.ranges(sid))
                if what == "status":
                    return self._send(200, svc.status(sid))
                if what == "export":
                    return self._stream(svc.store.iter_export(sid))
                after = int(q.get("after", ["-1"])[0])
                mode = q.get("mode", [None])[0]
                return self._stream(json.dumps(b, separators=(",", ":")) for b in svc.store.results(sid, mode, after))
            except Exception as e:  # noqa: BLE001
                log.exception("GET failed")
                self._send(500, {"error": repr(e)})

        def do_POST(self):
            try:
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n)
                if not self._auth():
                    return
                payload = decode_body(body, self.headers.get("Content-Encoding"))
                if self.path == "/v1/ingest":
                    return self._send(200, svc.ingest(payload))
                m = re.fullmatch(r"/v1/sessions/([^/]+)/results", self.path)
                if m:
                    return self._send(200, {"ok": True, "stored": svc.put_results(m.group(1), payload["results"])})
                return self._send(404, {"error": "not found"})
            except (KeyError, ValueError) as e:
                self._send(400, {"error": repr(e)})
            except Exception as e:  # noqa: BLE001
                log.exception("POST failed")
                self._send(500, {"error": repr(e)})

    return H


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64


def serve(host: str, port: int, db_path: str, api_key: str, sync: str = "FULL"):
    svc = CloudService(db_path, sync)
    srv = Server((host, port), make_handler(svc, api_key))
    log.info("cloud listening on %s:%d db=%s", host, port, db_path)
    srv.serve_forever()
