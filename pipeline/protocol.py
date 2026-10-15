"""Wire formats: the mock's NDJSON replies (with raw-text CRC check) and the device<->cloud payloads."""
from __future__ import annotations

import gzip
import json
import re
import zlib
from dataclasses import dataclass, field


class ProtocolError(Exception):
    pass


@dataclass
class Reply:
    session: str
    sensor: int
    first_index: int
    count: int
    lost: list = field(default_factory=list)     # (from, to|None, reason)
    session_end: bool = False
    data: list = field(default_factory=list)
    crc_ok: bool = True
    error: str = ""


_DATA_RE = re.compile(r'"data"\s*:\s*(\[(?:\s*\[[^\[\]]*\]\s*,?)*\s*\])')


def _raw_data_text(line: str) -> str:
    m = _DATA_RE.search(line)
    if m:
        return m.group(1)
    # fallback: bracket matching after the "data" key
    k = line.find('"data"')
    if k < 0:
        raise ProtocolError("no data field")
    i = line.find("[", k)
    depth = 0
    in_str = False
    esc = False
    for j in range(i, len(line)):
        ch = line[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return line[i:j + 1]
    raise ProtocolError("unterminated data array")


def parse_reply(raw: bytes) -> Reply:
    try:
        line = raw.decode("utf-8").strip()
        obj = json.loads(line)
    except (UnicodeDecodeError, ValueError) as e:
        raise ProtocolError(f"bad json: {e}") from e
    if not isinstance(obj, dict):
        raise ProtocolError("reply is not an object")
    if "error" in obj and "data" not in obj:
        return Reply(session="", sensor=int(obj.get("sensor", 0) or 0), first_index=0, count=0, error=str(obj["error"]))
    try:
        sensor = int(obj["sensor"])
        data = obj["data"]
        count = int(obj["count"])
        first = int(obj["first_index"])
        sid = str(obj["session_id"])
    except (KeyError, TypeError, ValueError) as e:
        raise ProtocolError(f"missing/invalid field: {e}") from e
    width = 3 if sensor == 1 else 2
    if sensor not in (1, 2) or not isinstance(data, list) or len(data) != count:
        raise ProtocolError("count/data mismatch")
    for r in data:
        if not isinstance(r, list) or len(r) != width or not all(isinstance(v, int) and not isinstance(v, bool) for v in r):
            raise ProtocolError("bad sample row")
    lost = []
    for L in obj.get("lost") or []:
        to = L.get("to_index")
        lost.append((int(L["from_index"]), None if to is None else int(to), str(L.get("reason") or "unspecified")))
    crc_ok = False
    try:
        text = _raw_data_text(line)
        want = obj.get("crc32")
        got = zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF
        if isinstance(want, int):
            crc_ok = want == got
        else:
            crc_ok = str(want).strip().lower().lstrip("0x") == f"{got:08x}".lstrip("0") or str(want).strip().lower() == f"{got:08x}"
    except ProtocolError:
        crc_ok = False
    return Reply(sid, sensor, first, count, lost, bool(obj.get("session_end", False)), data, crc_ok)


# ---- device <-> cloud --------------------------------------------------------------------------
def encode_body(obj) -> tuple[bytes, dict]:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    if len(raw) > 1024:
        return gzip.compress(raw, 3), {"Content-Encoding": "gzip", "Content-Type": "application/json"}
    return raw, {"Content-Type": "application/json"}


def decode_body(raw: bytes, encoding: str | None):
    if encoding == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8")) if raw else {}
