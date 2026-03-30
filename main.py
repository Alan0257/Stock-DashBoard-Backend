"""
股票儀表板後端 API — v2
前端報價改由後端 CORS proxy 轉發，解決 Yahoo Finance CORS 封鎖問題
"""

from fastapi import FastAPI, HTTPException, Depends, Header, Response
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
from datetime import datetime
import asyncio, time, os, json, logging
from typing import Optional
import firebase_admin
from firebase_admin import credentials, auth, firestore

# ── Firebase ──────────────────────────────
def init_firebase():
    if firebase_admin._apps:
        return
    fb = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if fb:
        cred = credentials.Certificate(json.loads(fb))
        firebase_admin.initialize_app(cred)
    else:
        kp = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(kp):
            firebase_admin.initialize_app(credentials.Certificate(kp))

init_firebase()

# ── App ───────────────────────────────────
app = FastAPI(title="Stock Dashboard API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Cache ─────────────────────────────────
_cache: dict = {}

def cache_get(key, ttl=60):
    item = _cache.get(key)
    if item and (time.time() - item["ts"]) < ttl:
        return item["data"]
    return None

def cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

# ── Symbol mapping ─────────────────────────
INDEX_MAP = {
    "TWII":"^TWII","TAIEX":"^TWII","N225":"^N225","KOSPI":"^KS11",
    "HSI":"^HSI","SPX":"^GSPC","DJI":"^DJI","IXIC":"^IXIC","TPEX":"^TPEX",
}

def to_symbol(code: str) -> str:
    u = code.upper()
    if u in INDEX_MAP:
        return INDEX_MAP[u]
    if code[:4].replace(".", "").isdigit() or code.replace(".", "").isdigit():
        return code + ".TW"
    return u

# ── yfinance download (bypasses proxy better than Ticker.history) ──
def fetch_quote(symbol: str) -> dict:
    df = yf.download(symbol, period="5d", interval="1d",
                     progress=False, auto_adjust=True)
    if df.empty:
        df = yf.download(symbol, period="1mo", interval="1d",
                         progress=False, auto_adjust=True)
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    closes = df["Close"].dropna()
    if len(closes) == 0:
        raise ValueError(f"No closes for {symbol}")
    last  = float(closes.iloc[-1])
    prev  = float(closes.iloc[-2]) if len(closes) >= 2 else last
    chg   = last - prev
    chgp  = chg / prev * 100 if prev else 0
    row   = df.iloc[-1]
    r2    = lambda x: round(float(x), 2) if x == x else round(last, 2)
    return {
        "price":      r2(last),
        "open":       r2(row.get("Open", last)),
        "high":       r2(row.get("High", last)),
        "low":        r2(row.get("Low",  last)),
        "volume":     int(row.get("Volume", 0) or 0),
        "change":     round(chg, 2),
        "change_pct": round(chgp, 2),
        "trade_date": str(df.index[-1].date()),
    }

# ── Health ────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "time": datetime.now().isoformat()}

@app.get("/health")
def health():
    return {"status": "healthy"}

# ── CORS Proxy for quotes ─────────────────
@app.get("/proxy/quotes")
async def proxy_quotes(codes: str):
    """
    前端改打這個端點取得報價，後端幫忙打 Yahoo Finance
    用法: /proxy/quotes?codes=2330,NVDA,N225
    """
    cache_key = f"proxy:{codes}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    results = []

    def _fetch_one(code):
        sym = to_symbol(code)
        try:
            data = fetch_quote(sym)
            return {"code": code, "symbol": sym, "ok": True, **data}
        except Exception as e:
            return {"code": code, "symbol": sym, "ok": False, "error": str(e)}

    # 用 asyncio 跑多個（同步 yfinance 包在 executor 裡）
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, _fetch_one, c) for c in code_list]
    results = await asyncio.gather(*tasks)

    out = {"quotes": list(results), "updated_at": datetime.now().isoformat()}
    cache_set(cache_key, out)
    return out

# ── Auth ──────────────────────────────────
async def verify_token(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization")
    try:
        return auth.verify_id_token(authorization.split(" ", 1)[1])
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))

# ── Watchlist ─────────────────────────────
@app.get("/api/watchlist")
async def get_watchlist(user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        doc = db.collection("watchlists").document(uid).get()
        if doc.exists:
            return doc.to_dict()
        default = {
            "uid": uid,
            "groups": [
                {"name": "台股核心", "stocks": ["2330","2454","2317","2308","3008"]},
                {"name": "國際指數", "stocks": ["N225","KOSPI","HSI","SPX"]},
                {"name": "美國科技", "stocks": ["NVDA","AAPL","MSFT","TSLA"]},
                {"name": "自訂",     "stocks": ["2382","006208"]},
            ],
            "updated_at": datetime.now().isoformat(),
        }
        db.collection("watchlists").document(uid).set(default)
        return default
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/watchlist")
async def update_watchlist(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        body.update({"uid": uid, "updated_at": datetime.now().isoformat()})
        db.collection("watchlists").document(uid).set(body)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/watchlist/add")
async def add_stock(body: dict, user: dict = Depends(verify_token)):
    uid, code = user["uid"], body.get("code","").strip().upper()
    group_name = body.get("group", "自訂")
    if not code:
        raise HTTPException(status_code=400, detail="code required")
    try:
        db = firestore.client()
        ref = db.collection("watchlists").document(uid)
        doc = ref.get()
        data = doc.to_dict() if doc.exists else {"uid": uid, "groups": [{"name":"自訂","stocks":[]}]}
        groups = data.get("groups", [])
        target = next((g for g in groups if g["name"] == group_name), None)
        if not target:
            target = {"name": group_name, "stocks": []}
            groups.append(target)
        if code not in target["stocks"]:
            target["stocks"].append(code)
        data.update({"groups": groups, "updated_at": datetime.now().isoformat()})
        ref.set(data)
        return {"status": "ok", "code": code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/watchlist/remove")
async def remove_stock(body: dict, user: dict = Depends(verify_token)):
    uid, code = user["uid"], body.get("code","").strip().upper()
    try:
        db = firestore.client()
        ref = db.collection("watchlists").document(uid)
        doc = ref.get()
        if not doc.exists:
            return {"status": "ok"}
        data = doc.to_dict()
        for g in data.get("groups", []):
            g["stocks"] = [s for s in g["stocks"] if s.upper() != code]
        data["updated_at"] = datetime.now().isoformat()
        ref.set(data)
        return {"status": "ok", "removed": code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
