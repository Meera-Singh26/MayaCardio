"""Tiny persistent-connection HTTP(S) client for the cloud service."""
from __future__ import annotations

import http.client
import json
import threading
import urllib.parse

from ..protocol import encode_body


class NetError(Exception):
    pass


class CloudClient:
    def __init__(self, url: str, api_key: str, timeout: float = 3.0):
        u = urllib.parse.urlparse(url)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port or (443 if self.https else 80)
        self.base = u.path.rstrip("/")
        self.key, self.timeout = api_key, timeout
        self.conn = None
        self.lock = threading.Lock()
        self.bytes_sent = 0
        self.bytes_recv = 0

    def _new(self, timeout):
        cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
        return cls(self.host, self.port, timeout=timeout)

    def close(self):
        try:
            if self.conn:
                self.conn.close()
        except OSError:
            pass
        self.conn = None

    def request(self, method, path, body=None, headers=None, timeout=None):
        h = {"X-API-Key": self.key}
        h.update(headers or {})
        with self.lock:
            try:
                if self.conn is None:
                    self.conn = self._new(timeout or self.timeout)
                if timeout:
                    self.conn.timeout = timeout
                    if self.conn.sock:
                        self.conn.sock.settimeout(timeout)
                self.conn.request(method, self.base + path, body=body, headers=h)
                resp = self.conn.getresponse()
                data = resp.read()
            except (OSError, http.client.HTTPException) as e:
                self.close()
                raise NetError(repr(e)) from e
            self.bytes_sent += len(body or b"") + 200
            self.bytes_recv += len(data) + 150
            if resp.status >= 500:
                self.close()
                raise NetError(f"HTTP {resp.status}")
            if resp.status >= 400:
                raise NetError(f"HTTP {resp.status}: {data[:200]!r}")
            return data

    def get_json(self, path, timeout=None):
        return json.loads(self.request("GET", path, timeout=timeout) or b"{}")

    def post_json(self, path, obj, timeout=None):
        body, hdr = encode_body(obj)
        return json.loads(self.request("POST", path, body, hdr, timeout) or b"{}")

    def stream_lines(self, path, timeout=60):
        """Stream an NDJSON response line by line on a dedicated connection."""
        conn = self._new(timeout)
        try:
            conn.request("GET", self.base + path, headers={"X-API-Key": self.key})
            resp = conn.getresponse()
            if resp.status >= 400:
                raise NetError(f"HTTP {resp.status}")
            for line in resp:
                yield line.decode().rstrip("\n")
        except (OSError, http.client.HTTPException) as e:
            raise NetError(repr(e)) from e
        finally:
            conn.close()
