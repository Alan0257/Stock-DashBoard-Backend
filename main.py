"""
股票儀表板後端 API
技術棧: Python + FastAPI + yfinance + Firebase Admin
部署: Render (Web Service)
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import time
import os
import json
import logging
from typing import Optional, List
import httpx

# Firebase Admin
import firebase_admin
from firebase_admin import credentials, auth, firestore

# ─────────────────────────────────────────
# 初始化 Firebase Admin SDK
# ─────────────────────────────────────────
def init_firebase():
    """從環境變數讀取 Firebase 憑證並初始化"""
    if firebase_admin._apps:
        return  # 已經初始化過了

    firebase_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if firebase_json:
        cred_dict = json.loads(firebase_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        logging.info("Firebase initialized from environment variable")
    else:
        # 本地開發：放一個 serviceAccountKey.json 在同目錄
        key_path = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(key_path):
            cred = credentials.Certificate(key_path)
            firebase_admin.initialize_app(cred)
            logging.info("Firebase initialized from serviceAccountKey.json")
        else:
            logging.warning("Firebase not initialized — no credentials found. Auth endpoints will fail.")

init_firebase()

# ─────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────
app = FastAPI(
    title="股票儀表板 API",
    description="台股 + 美股即時報價、K線、籌碼、自選股管理",
    version="1.0.0"
)

# CORS：允許你的 GitHub Pages 網域
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────
# 簡易快取（避免重複打 Yahoo Finance）
# ─────────────────────────────────────────
_cache: dict = {}
CACHE_TTL = 60  # 秒

def cache_get(key: str):
    item = _cache.get(key)
    if item and (time.time() - item["ts"]) < CACHE_TTL:
        return item["data"]
    return None

def cache_set(key: str, data):
    _cache[key] = {"ts": time.time(), "data": data}

# ─────────────────────────────────────────
# 台股代號轉換 (2330 → 2330.TW)
# ─────────────────────────────────────────
def to_yf_symbol(code: str) -> str:
    """
    將台股代號轉換為 yfinance 格式
    - 純數字 → 加 .TW (上市) 或 .TWO (上櫃，以後可擴充)
    - 指數代碼直接對應
    - 美股保持原樣
    """
    INDEX_MAP = {
        "TAIEX": "^TWII",     # 台灣加權指數
        "TWII":  "^TWII",
        "N225":  "^N225",     # 日經225
        "KOSPI": "^KS11",     # 韓KOSPI
        "HSI":   "^HSI",      # 恒生
        "SPX":   "^GSPC",     # S&P500
        "DJI":   "^DJI",      # 道瓊
        "IXIC":  "^IXIC",     # NASDAQ
        "TPEX":  "^TPEX",     # 櫃買
    }
    if code.upper() in INDEX_MAP:
        return INDEX_MAP[code.upper()]
    # 純數字 = 台股
    if code.replace(".", "").isdigit() or (len(code) >= 4 and code[:4].isdigit()):
        return code + ".TW"
    # 其他 = 美股
    return code.upper()

# ─────────────────────────────────────────
# Firebase Auth 驗證 middleware
# ─────────────────────────────────────────
async def verify_token(authorization: Optional[str] = Header(None)) -> dict:
    """
    驗證前端傳來的 Firebase ID Token
    Header 格式: Authorization: Bearer <token>
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.split(" ", 1)[1]
    try:
        decoded = auth.verify_id_token(token)
        return decoded
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

# ─────────────────────────────────────────
# 健康檢查
# ─────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "message": "股票儀表板 API 運行中", "time": datetime.now().isoformat()}

@app.get("/health")
def health():
    return {"status": "healthy"}

# ─────────────────────────────────────────
# 報價 API
# ─────────────────────────────────────────
@app.get("/api/quote/{code}")
async def get_quote(code: str):
    """
    取得單支股票即時報價
    支援: 台股(2330)、指數(N225, KOSPI, SPX)、美股(NVDA, AAPL)
    """
    cache_key = f"quote:{code}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    symbol = to_yf_symbol(code)
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info

        # 取最近一天的 K 線當作即時資料
        hist = ticker.history(period="2d", interval="1d")
        if hist.empty:
            raise HTTPException(status_code=404, detail=f"找不到 {code} 的資料")

        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else float(hist["Close"].iloc[-1])
        last_close = float(hist["Close"].iloc[-1])
        today = hist.iloc[-1]

        change = last_close - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0

        result = {
            "code": code,
            "symbol": symbol,
            "price": round(last_close, 2),
            "open": round(float(today["Open"]), 2),
            "high": round(float(today["High"]), 2),
            "low": round(float(today["Low"]), 2),
            "volume": int(today["Volume"]),
            "prev_close": round(prev_close, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "updated_at": datetime.now().isoformat(),
        }
        cache_set(cache_key, result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"取得報價失敗: {str(e)}")


@app.get("/api/quotes")
async def get_quotes(codes: str):
    """
    批次取得多支股票報價
    用法: /api/quotes?codes=2330,2454,NVDA,N225
    """
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if len(code_list) > 30:
        raise HTTPException(status_code=400, detail="最多一次查詢 30 支")

    tasks = [get_quote(code) for code in code_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    data = []
    for code, result in zip(code_list, results):
        if isinstance(result, Exception):
            data.append({"code": code, "error": str(result)})
        else:
            data.append(result)
    return {"quotes": data, "count": len(data)}


# ─────────────────────────────────────────
# K 線資料 API
# ─────────────────────────────────────────
INTERVAL_MAP = {
    "1m": "1m",   "3m": "2m",   "5m": "5m",
    "15m": "15m", "30m": "30m", "60m": "60m",
    "1h": "1h",
    "D": "1d", "W": "1wk", "M": "1mo",
}
PERIOD_MAP = {
    "1m": "1d",  "3m": "5d",   "5m": "5d",
    "15m": "5d", "30m": "1mo", "60m": "3mo",
    "1h": "3mo",
    "D": "1y",   "W": "2y",    "M": "5y",
}

@app.get("/api/kline/{code}")
async def get_kline(code: str, tf: str = "D"):
    """
    取得 K 線資料
    tf: 1m / 5m / 15m / 30m / 60m / D / W / M
    """
    if tf not in INTERVAL_MAP:
        raise HTTPException(status_code=400, detail=f"不支援的時間框架: {tf}，可用: {list(INTERVAL_MAP.keys())}")

    cache_key = f"kline:{code}:{tf}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    symbol = to_yf_symbol(code)
    interval = INTERVAL_MAP[tf]
    period = PERIOD_MAP[tf]

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)

        if hist.empty:
            raise HTTPException(status_code=404, detail=f"找不到 {code} 的 K 線資料")

        candles = []
        for ts, row in hist.iterrows():
            candles.append({
                "t": int(ts.timestamp() * 1000),  # milliseconds for JS
                "o": round(float(row["Open"]), 4),
                "h": round(float(row["High"]), 4),
                "l": round(float(row["Low"]), 4),
                "c": round(float(row["Close"]), 4),
                "v": int(row["Volume"]),
            })

        result = {
            "code": code,
            "symbol": symbol,
            "tf": tf,
            "candles": candles,
            "count": len(candles),
        }
        cache_set(cache_key, result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"取得 K 線失敗: {str(e)}")


# ─────────────────────────────────────────
# 股票基本資訊 API
# ─────────────────────────────────────────
@app.get("/api/info/{code}")
async def get_stock_info(code: str):
    """取得股票基本資訊（本益比、市值、產業等）"""
    cache_key = f"info:{code}"
    cached = cache_get(cached) if False else None  # 基本資訊快取 10 分鐘
    item = _cache.get(cache_key)
    if item and (time.time() - item["ts"]) < 600:
        return item["data"]

    symbol = to_yf_symbol(code)
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        result = {
            "code": code,
            "name": info.get("longName") or info.get("shortName", code),
            "sector": info.get("sector", "—"),
            "industry": info.get("industry", "—"),
            "pe_ratio": info.get("trailingPE"),
            "pb_ratio": info.get("priceToBook"),
            "dividend_yield": round(info.get("dividendYield", 0) * 100, 2) if info.get("dividendYield") else None,
            "market_cap": info.get("marketCap"),
            "shares_outstanding": info.get("sharesOutstanding"),
            "week52_high": info.get("fiftyTwoWeekHigh"),
            "week52_low": info.get("fiftyTwoWeekLow"),
            "avg_volume": info.get("averageVolume"),
            "currency": info.get("currency", "TWD"),
        }
        _cache[cache_key] = {"ts": time.time(), "data": result}
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"取得股票資訊失敗: {str(e)}")


# ─────────────────────────────────────────
# 自選股管理 API（需要 Firebase 認證）
# ─────────────────────────────────────────
@app.get("/api/watchlist")
async def get_watchlist(user: dict = Depends(verify_token)):
    """取得目前使用者的自選股清單"""
    uid = user["uid"]
    try:
        db = firestore.client()
        doc = db.collection("watchlists").document(uid).get()
        if doc.exists:
            return doc.to_dict()
        else:
            # 第一次登入，建立預設自選股
            default = {
                "uid": uid,
                "groups": [
                    {"name": "台股核心", "stocks": ["2330", "2454", "2317", "2308", "3008"]},
                    {"name": "國際指數", "stocks": ["N225", "KOSPI", "HSI", "SPX"]},
                    {"name": "美國科技", "stocks": ["NVDA", "AAPL", "MSFT", "TSLA"]},
                    {"name": "自訂", "stocks": ["2382", "006208"]},
                ],
                "updated_at": datetime.now().isoformat(),
            }
            db.collection("watchlists").document(uid).set(default)
            return default
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取自選股失敗: {str(e)}")


@app.put("/api/watchlist")
async def update_watchlist(
    body: dict,
    user: dict = Depends(verify_token)
):
    """更新使用者自選股清單（完整覆蓋）"""
    uid = user["uid"]
    try:
        db = firestore.client()
        body["uid"] = uid
        body["updated_at"] = datetime.now().isoformat()
        db.collection("watchlists").document(uid).set(body)
        return {"status": "ok", "message": "自選股已儲存"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"儲存自選股失敗: {str(e)}")


@app.post("/api/watchlist/add")
async def add_to_watchlist(
    body: dict,
    user: dict = Depends(verify_token)
):
    """
    新增一支股票到指定群組
    body: { "code": "2330", "group": "台股核心" }
    """
    uid = user["uid"]
    code = body.get("code", "").strip().upper()
    group_name = body.get("group", "自訂")

    if not code:
        raise HTTPException(status_code=400, detail="code 不能為空")

    try:
        db = firestore.client()
        ref = db.collection("watchlists").document(uid)
        doc = ref.get()

        if doc.exists:
            data = doc.to_dict()
        else:
            data = {"uid": uid, "groups": [{"name": "自訂", "stocks": []}]}

        # 找到目標群組，若不存在則建立
        groups = data.get("groups", [])
        target = next((g for g in groups if g["name"] == group_name), None)
        if target is None:
            target = {"name": group_name, "stocks": []}
            groups.append(target)

        if code not in target["stocks"]:
            target["stocks"].append(code)

        data["groups"] = groups
        data["updated_at"] = datetime.now().isoformat()
        ref.set(data)
        return {"status": "ok", "code": code, "group": group_name}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/watchlist/remove")
async def remove_from_watchlist(
    body: dict,
    user: dict = Depends(verify_token)
):
    """
    從自選股移除一支
    body: { "code": "2330" }
    """
    uid = user["uid"]
    code = body.get("code", "").strip().upper()

    try:
        db = firestore.client()
        ref = db.collection("watchlists").document(uid)
        doc = ref.get()
        if not doc.exists:
            return {"status": "ok"}

        data = doc.to_dict()
        for group in data.get("groups", []):
            group["stocks"] = [s for s in group["stocks"] if s.upper() != code]

        data["updated_at"] = datetime.now().isoformat()
        ref.set(data)
        return {"status": "ok", "removed": code}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────
# 三大法人（TWSE OpenAPI）
# ─────────────────────────────────────────
@app.get("/api/chip/{code}")
async def get_chip(code: str):
    """
    取得三大法人買賣超
    僅支援台股代號（純數字）
    """
    if not code[:4].isdigit():
        raise HTTPException(status_code=400, detail="三大法人資料僅支援台股代號")

    cache_key = f"chip:{code}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # TWSE OpenAPI
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            raw = resp.json()

        rows = raw.get("data", [])
        target = next((r for r in rows if r[0] == code), None)

        if not target:
            raise HTTPException(status_code=404, detail=f"找不到 {code} 的法人資料")

        def parse_num(s):
            try:
                return int(s.replace(",", ""))
            except:
                return 0

        result = {
            "code": code,
            "date": raw.get("date", ""),
            "foreign_buy": parse_num(target[4]),
            "foreign_sell": parse_num(target[5]),
            "foreign_net": parse_num(target[6]),
            "trust_buy": parse_num(target[7]),
            "trust_sell": parse_num(target[8]),
            "trust_net": parse_num(target[9]),
            "dealer_net": parse_num(target[12]),
            "total_net": parse_num(target[18]),
        }
        cache_set(cache_key, result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"取得法人資料失敗: {str(e)}")
