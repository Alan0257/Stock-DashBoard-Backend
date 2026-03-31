"""
股票儀表板後端 API v3
- httpx + proxy=None 直連 Yahoo Finance，繞過 Render 系統 proxy
- 路由: GET /health  GET /api/quote/{code}  GET /api/quotes  GET|PUT /api/watchlist  POST /api/watchlist/add  DELETE /api/watchlist/remove
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import asyncio, time, os, json, logging
from typing import Optional
import httpx
import firebase_admin
from firebase_admin import credentials, auth, firestore

logging.basicConfig(level=logging.INFO)

def init_firebase():
    if firebase_admin._apps:
        return
    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if raw:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(raw)))
    else:
        kp = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(kp):
            firebase_admin.initialize_app(credentials.Certificate(kp))

init_firebase()

app = FastAPI(title="Stock Dashboard API", version="3.0.0")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

_cache: dict = {}
def cache_get(key, ttl=60):
    i = _cache.get(key)
    return i["data"] if i and (time.time()-i["ts"]) < ttl else None
def cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

INDEX_MAP = {"TWII":"^TWII","TAIEX":"^TWII","N225":"^N225","KOSPI":"^KS11",
             "HSI":"^HSI","SPX":"^GSPC","DJI":"^DJI","IXIC":"^IXIC","TPEX":"^TPEX"}

def to_yf(code):
    u = code.upper()
    if u in INDEX_MAP: return INDEX_MAP[u]
    if code.replace(".","").isdigit() or (len(code)>=4 and code[:4].isdigit()): return code+".TW"
    return u

_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://finance.yahoo.com/",
}

async def _fetch(code: str) -> dict:
    symbol = to_yf(code)
    cached = cache_get(f"q:{code}")
    if cached: return cached
    transport = httpx.AsyncHTTPTransport(proxy=None)
    for host in ["query1","query2"]:
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        try:
            async with httpx.AsyncClient(transport=transport, timeout=20, follow_redirects=True) as c:
                r = await c.get(url, headers=_HDR)
            logging.info(f"Yahoo {host}/{symbol} => {r.status_code}")
            if r.status_code != 200: continue
            d = r.json()
            res = (d.get("chart") or {}).get("result") or []
            if not res: continue
            q = res[0]["indicators"]["quote"][0]
            def clean(l): return [v for v in l if v is not None]
            closes = clean(q.get("close",[]))
            if not closes: continue
            last, prev = closes[-1], (closes[-2] if len(closes)>=2 else closes[-1])
            chg = last-prev; chgp = chg/prev*100 if prev else 0
            opens=clean(q.get("open",[])); highs=clean(q.get("high",[])); lows=clean(q.get("low",[])); vols=clean(q.get("volume",[]))
            r2 = lambda x: round(x,2)
            result = {"code":code,"symbol":symbol,"price":r2(last),
                      "open":r2(opens[-1] if opens else last),"high":r2(highs[-1] if highs else last),
                      "low":r2(lows[-1] if lows else last),"volume":int(vols[-1] if vols else 0),
                      "change":r2(chg),"change_pct":r2(chgp),"updated_at":datetime.now().isoformat()}
            cache_set(f"q:{code}", result)
            return result
        except Exception as e:
            logging.warning(f"_fetch {host}/{symbol}: {e}")
    raise HTTPException(status_code=404, detail=f"找不到 {code} 的報價")

@app.get("/")
def root(): return {"status":"ok","version":"3.0.0","time":datetime.now().isoformat()}

@app.get("/health")
def health(): return {"status":"healthy"}

@app.get("/api/quote/{code}")
async def get_quote(code: str): return await _fetch(code)

@app.get("/api/quotes")
async def get_quotes(codes: str):
    cl = [c.strip() for c in codes.split(",") if c.strip()]
    if len(cl) > 30: raise HTTPException(400, "最多30支")
    results = await asyncio.gather(*[_fetch(c) for c in cl], return_exceptions=True)
    return {"quotes":[r if not isinstance(r,Exception) else {"code":cl[i],"error":str(r)} for i,r in enumerate(results)],
            "count":len(cl),"updated_at":datetime.now().isoformat()}

async def verify_token(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401,"Missing Authorization")
    try: return auth.verify_id_token(authorization.split(" ",1)[1])
    except Exception as e: raise HTTPException(401, str(e))

DEFAULT_GROUPS = [
    {"name":"台股核心","stocks":["2330","2454","2317","2308","3008"]},
    {"name":"國際指數","stocks":["N225","KOSPI","HSI","SPX"]},
    {"name":"美國科技","stocks":["NVDA","AAPL","MSFT","TSLA"]},
    {"name":"自訂","stocks":["2382","006208"]},
]

@app.get("/api/watchlist")
async def get_watchlist(user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        doc = db.collection("watchlists").document(uid).get()
        if doc.exists: return doc.to_dict()
        default = {"uid":uid,"groups":DEFAULT_GROUPS,"updated_at":datetime.now().isoformat()}
        db.collection("watchlists").document(uid).set(default)
        return default
    except Exception as e: raise HTTPException(500, str(e))

@app.put("/api/watchlist")
async def update_watchlist(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        body["uid"]=uid; body["updated_at"]=datetime.now().isoformat()
        db.collection("watchlists").document(uid).set(body)
        return {"status":"ok"}
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/api/watchlist/add")
async def add_stock(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]; code = body.get("code","").strip().upper(); grp = body.get("group","自訂")
    if not code: raise HTTPException(400,"code不能為空")
    try:
        db = firestore.client(); ref = db.collection("watchlists").document(uid)
        doc = ref.get(); data = doc.to_dict() if doc.exists else {"uid":uid,"groups":DEFAULT_GROUPS}
        groups = data.get("groups",[])
        target = next((g for g in groups if g["name"]==grp), None)
        if not target: target={"name":grp,"stocks":[]}; groups.append(target)
        if code not in target["stocks"]: target["stocks"].append(code)
        data["groups"]=groups; data["updated_at"]=datetime.now().isoformat()
        ref.set(data); return {"status":"ok","code":code,"group":grp}
    except Exception as e: raise HTTPException(500, str(e))

@app.delete("/api/watchlist/remove")
async def remove_stock(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]; code = body.get("code","").strip().upper()
    try:
        db = firestore.client(); ref = db.collection("watchlists").document(uid)
        doc = ref.get()
        if not doc.exists: return {"status":"ok"}
        data = doc.to_dict()
        for g in data.get("groups",[]): g["stocks"]=[s for s in g["stocks"] if s.upper()!=code]
        data["updated_at"]=datetime.now().isoformat(); ref.set(data)
        return {"status":"ok","removed":code}
    except Exception as e: raise HTTPException(500, str(e))

# ═══════════════════════════════════════════════════════════════════
# 台股補充資料 API（TWSE OpenAPI，proxy=None 直連）
# ═══════════════════════════════════════════════════════════════════

_TWSE_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

async def _twse_get(url: str, ttl: int = 120) -> dict:
    """打 TWSE/TDCC API，手動處理 307 redirect，proxy=None 直連"""
    cached = cache_get(f"twse:{url}", ttl)
    if cached:
        return cached
    transport = httpx.AsyncHTTPTransport(proxy=None)
    current_url = url
    # 手動最多跟 3 次 redirect（307/302/301）
    for _ in range(4):
        async with httpx.AsyncClient(transport=transport, timeout=15,
                                     follow_redirects=False) as c:
            r = await c.get(current_url, headers=_TWSE_HDR)
        if r.status_code in (301, 302, 307, 308):
            current_url = str(r.headers.get("location", current_url))
            logging.info(f"TWSE redirect → {current_url}")
            continue
        if r.status_code == 200:
            data = r.json()
            cache_set(f"twse:{url}", data)
            return data
        raise HTTPException(r.status_code, f"TWSE API error {r.status_code}")
    raise HTTPException(500, "Too many redirects")


# ── 三大法人買賣超 ────────────────────────────────────────────────
@app.get("/api/chip/{code}")
async def get_chip(code: str):
    """
    三大法人買賣超（TWSE OpenAPI）
    僅支援台股上市股票（純數字代號）
    """
    if not code[:4].isdigit():
        raise HTTPException(400, "三大法人僅支援台股上市代號")
    try:
        # 今日三大法人
        data = await _twse_get(
            "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
        )
        rows = data.get("data", [])
        row = next((r for r in rows if r[0] == code), None)
        if not row:
            raise HTTPException(404, f"找不到 {code} 的法人資料")

        def pn(s):
            try: return int(str(s).replace(",", ""))
            except: return 0

        foreign_net = pn(row[6])
        trust_net   = pn(row[9])
        dealer_net  = pn(row[12])

        # 計算連買賣天數（近 30 日資料）
        hist = await _twse_get(
            f"https://www.twse.com.tw/rwd/zh/fund/TWT38U?response=json&stockNo={code}"
        )
        hist_rows = hist.get("data", [])

        def streak(rows, col_idx):
            """連買(正)或連賣(負)天數"""
            s = 0
            for r in reversed(rows[-30:]):
                try:
                    v = int(str(r[col_idx]).replace(",",""))
                except:
                    break
                if s == 0:
                    s = 1 if v > 0 else (-1 if v < 0 else 0)
                elif s > 0 and v > 0:
                    s += 1
                elif s < 0 and v < 0:
                    s -= 1
                else:
                    break
            return s

        foreign_streak = streak(hist_rows, 4)   # 外資淨買超欄
        trust_streak   = streak(hist_rows, 7)   # 投信淨買超欄

        result = {
            "code": code,
            "date": data.get("date", ""),
            "foreign_net":    foreign_net,
            "trust_net":      trust_net,
            "dealer_net":     dealer_net,
            "total_net":      foreign_net + trust_net + dealer_net,
            "foreign_streak": foreign_streak,   # 正=連買N日，負=連賣N日
            "trust_streak":   trust_streak,
        }
        return result
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"chip {code}: {e}")
        raise HTTPException(500, str(e))


# ── 股票基本資料（Yahoo Finance info）────────────────────────────
@app.get("/api/info/{code}")
async def get_info(code: str):
    """股票基本資料，來源：Yahoo Finance v11 quoteSummary"""
    cached = cache_get(f"info:{code}", 600)
    if cached:
        return cached

    symbol = to_yf(code)
    transport = httpx.AsyncHTTPTransport(proxy=None)

    # 試 v11，失敗換 v10
    for ver in ["v11", "v10"]:
        url = (f"https://query1.finance.yahoo.com/{ver}/finance/quoteSummary/{symbol}"
               f"?modules=summaryDetail%2CdefaultKeyStatistics%2CfinancialData%2CassetProfile")
        try:
            async with httpx.AsyncClient(transport=transport, timeout=20,
                                         follow_redirects=True) as c:
                r = await c.get(url, headers=_HDR)
            if r.status_code != 200:
                continue
            d = (r.json().get("quoteSummary") or {}).get("result") or [{}]
            d = d[0] if d else {}
            sd = d.get("summaryDetail") or {}
            ks = d.get("defaultKeyStatistics") or {}
            fd = d.get("financialData") or {}
            ap = d.get("assetProfile") or {}

            def val(obj, key):
                v = obj.get(key)
                return v.get("raw") if isinstance(v, dict) else v

            def pct(obj, key):
                v = val(obj, key)
                return round(v * 100, 2) if v is not None else None

            result = {
                "code": code, "symbol": symbol,
                "pe_ratio":           round(val(sd,"trailingPE"),1) if val(sd,"trailingPE") else None,
                "pb_ratio":           round(val(ks,"priceToBook"),2) if val(ks,"priceToBook") else None,
                "dividend_yield":     pct(sd,"dividendYield"),
                "market_cap":         val(sd,"marketCap"),
                "shares_outstanding": val(ks,"sharesOutstanding"),
                "week52_high":        val(sd,"fiftyTwoWeekHigh"),
                "week52_low":         val(sd,"fiftyTwoWeekLow"),
                "sector":             ap.get("sector") or ap.get("industry"),
                "industry":           ap.get("industry"),
                "profit_margins":     pct(fd,"profitMargins"),
                "gross_margins":      pct(fd,"grossMargins"),
                "return_on_equity":   pct(fd,"returnOnEquity"),
            }
            cache_set(f"info:{code}", result)
            return result
        except Exception as e:
            logging.warning(f"info {ver}/{symbol}: {e}")
            continue

    raise HTTPException(404, f"無法取得 {code} 基本資料")


# ── 集保大戶分佈（TDCC OpenAPI）──────────────────────────────────
@app.get("/api/holders/{code}")
async def get_holders(code: str):
    """集保大戶持股分佈，來源：TDCC OpenAPI"""
    if not code[:4].isdigit():
        raise HTTPException(400, "集保資料僅支援台股")
    try:
        data = await _twse_get(
            f"https://openapi.tdcc.com.tw/v1/opendata/1-5?StockNo={code}",
            ttl=86400   # 每週更新，快取 24hr
        )
        if not data:
            raise HTTPException(404, f"找不到 {code} 的集保資料")

        # 取最新一筆（API 回傳多週）
        latest_week = data[0].get("CalculationDate", "")
        week_data = [r for r in data if r.get("CalculationDate") == latest_week]

        brackets = []
        for row in week_data:
            brackets.append({
                "level":    row.get("HolderLevel", ""),
                "holders":  int(row.get("HolderCount", 0) or 0),
                "shares":   int(row.get("ShareCount", 0) or 0),
                "pct":      float(row.get("SharesPercent", 0) or 0),
            })

        # 400張以上大戶
        whale_pct = sum(b["pct"] for b in brackets
                        if b["level"] in ["400-599","600-800","800-1000","超過1000"])

        return {
            "code":       code,
            "week":       latest_week,
            "brackets":   brackets,
            "whale_pct":  round(whale_pct, 2),
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"holders {code}: {e}")
        raise HTTPException(500, str(e))

# ── 股票搜尋（Yahoo Finance search API）──────────────────────────
@app.get("/api/search")
async def search_stock(q: str):
    """
    搜尋股票代號或名稱
    用法: /api/search?q=台積電 或 /api/search?q=2330
    """
    if not q or len(q.strip()) < 1:
        raise HTTPException(400, "搜尋字串不能為空")

    cached = cache_get(f"search:{q}", 300)
    if cached:
        return cached

    transport = httpx.AsyncHTTPTransport(proxy=None)
    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={q}&lang=zh-TW&region=TW&quotesCount=8&newsCount=0"

    try:
        async with httpx.AsyncClient(transport=transport, timeout=10,
                                     follow_redirects=True) as c:
            r = await c.get(url, headers=_HDR)
        if r.status_code != 200:
            raise HTTPException(r.status_code, "搜尋失敗")

        quotes = r.json().get("quotes", [])
        results = []
        for item in quotes:
            sym = item.get("symbol", "")
            # 只留台股(.TW/.TWO)和美股(無後綴)
            if not (sym.endswith(".TW") or sym.endswith(".TWO") or
                    ("." not in sym and item.get("quoteType") in ("EQUITY","ETF","INDEX"))):
                continue
            # 還原回前端用的 code
            code = sym.replace(".TW","").replace(".TWO","")
            results.append({
                "code":     code,
                "symbol":   sym,
                "name":     item.get("longname") or item.get("shortname") or code,
                "exchange": item.get("exchange",""),
                "type":     item.get("quoteType",""),
            })

        out = {"query": q, "results": results[:8]}
        cache_set(f"search:{q}", out)
        return out

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"search {q}: {e}")
        raise HTTPException(500, str(e))
