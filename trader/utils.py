from __future__ import annotations
import hmac, hashlib, time, os, json
from typing import Any, Dict
import orjson

def now_ms() -> int:
    return int(time.time() * 1000)

def hmac_sha256(secret: str, msg: str) -> str:
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

def to_json(obj: Any) -> str:
    return orjson.dumps(obj).decode()

def from_json(s: str) -> Any:
    return orjson.loads(s)

def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)

class StrEnum(str):
    pass
