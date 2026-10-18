import base64
import binascii
import itertools
import math
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

TOTAL_ORDERS = 49      # fixed catalog: ids 1..49
LIMIT = 17             # requests
WINDOW = 10.0          # seconds
MAX_PAGE = 100


# ---------------------------------------------------------------- rate limit
class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.hits = defaultdict(deque)
        self.calls = 0

    async def dispatch(self, request: Request, call_next):
        client_id = request.headers.get("x-client-id")
        if request.method == "OPTIONS" or not client_id:
            return await call_next(request)

        now = time.monotonic()
        self.calls += 1
        if self.calls % 500 == 0:  # drop idle buckets
            for k in [k for k, q in self.hits.items() if not q or now - q[-1] >= WINDOW]:
                del self.hits[k]

        q = self.hits[client_id]
        while q and now - q[0] >= WINDOW:
            q.popleft()
        if len(q) >= LIMIT:
            retry = max(1, math.ceil(WINDOW - (now - q[0])))
            return JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(retry)},
            )
        q.append(now)
        return await call_next(request)


app = FastAPI(title="Orders API")

# Last added = outermost: CORS -> RateLimit -> routes, so 429s carry CORS headers too.
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotent-Replayed"],
    allow_credentials=False,
)

# -------------------------------------------------------------------- data
CATALOG = [
    {"id": i, "status": "confirmed", "name": f"Order {i}", "total": round(10 + i * 2.5, 2)}
    for i in range(1, TOTAL_ORDERS + 1)
]

# Orders created through POST get ids from 1001 so they never join the fixed 1..T catalog.
_new_ids = itertools.count(1001)
_created_orders = {}   # id -> order
_by_key = {}           # Idempotency-Key -> order


# --------------------------------------------------------------- idempotent
@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: Optional[str] = Header(default=None),
):
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    existing = _by_key.get(idempotency_key)
    if existing is not None:
        return JSONResponse(existing, status_code=201, headers={"Idempotent-Replayed": "true"})

    try:
        body = await request.json()
    except Exception:
        body = None
    # No await between the check above and the store below, so this is atomic per process.
    order = {
        "id": next(_new_ids),
        "status": "created",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "details": body if isinstance(body, dict) else None,
    }
    _created_orders[order["id"]] = order
    _by_key[idempotency_key] = order
    return order


# --------------------------------------------------------------- pagination
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        prefix, num = raw.split(":", 1)
        offset = int(num)
        if prefix != "o" or offset < 0:
            raise ValueError
        return offset
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="invalid cursor")


@app.get("/orders")
async def list_orders(limit: int = 10, cursor: Optional[str] = None):
    limit = max(1, min(limit, MAX_PAGE))
    offset = decode_cursor(cursor) if cursor else 0
    items = CATALOG[offset: offset + limit]
    end = offset + len(items)
    next_cursor = encode_cursor(end) if end < len(CATALOG) else None
    return {"items": items, "next_cursor": next_cursor}


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    if 1 <= order_id <= TOTAL_ORDERS:
        return CATALOG[order_id - 1]
    if order_id in _created_orders:
        return _created_orders[order_id]
    raise HTTPException(status_code=404, detail="order not found")
