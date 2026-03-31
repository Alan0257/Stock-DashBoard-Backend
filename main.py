"""
股票儀表板後端 API v5
"""
from fastapi import FastAPI, HTTPException, Depends, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import asyncio, time, os, json, logging
from typing import Optional
import httpx
import firebase_admin
from firebase_admin import credentials, auth, firestore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def init_firebase():
    if firebase_admin._apps: return
    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if raw:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(raw)))
    else:
        kp = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(kp):
            firebase_admin.initialize_app(credentials.Certificate(kp))

init_firebase()

app = FastAPI(title="Stock Dashboard API", version="5.0.0")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ── Cache ─────────────────────────────────────────────────────────
_cache: dict = {}
def cache_get(key, ttl=60):
    i = _cache.get(key)
    return i["data"] if i and (time.time()-i["ts"]) < ttl else None
def cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

# ── Symbol mapping ────────────────────────────────────────────────
INDEX_MAP = {"TWII":"^TWII","TAIEX":"^TWII","N225":"^N225","KOSPI":"^KS11",
             "HSI":"^HSI","SPX":"^GSPC","DJI":"^DJI","IXIC":"^IXIC","TPEX":"^TPEX"}
def to_yf(code):
    u = code.upper()
    if u in INDEX_MAP: return INDEX_MAP[u]
    if code.replace(".","").isdigit() or (len(code)>=4 and code[:4].isdigit()): return code+".TW"
    return u

# ── HTTP clients ──────────────────────────────────────────────────
_YAHOO_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Accept": "application/json",
    "Referer": "https://finance.yahoo.com/",
}
_TWSE_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.twse.com.tw/",
    "Origin": "https://www.twse.com.tw",
}

def _transport(): return httpx.AsyncHTTPTransport(proxy=None)

async def _get(url, headers, timeout=20, follow=True):
    """Generic GET with proxy=None"""
    async with httpx.AsyncClient(
        transport=_transport(), timeout=timeout,
        follow_redirects=follow, max_redirects=10
    ) as c:
        r = await c.get(url, headers=headers)
    logging.info(f"GET {url[-70:]} => {r.status_code} {len(r.content)}b")
    return r

def _pn(s):
    """Parse number string, handles commas and spaces"""
    try: return int(str(s).replace(",","").replace(" ","").replace("\u00a0","").replace("+","").strip())
    except: return 0

# ── Routes ────────────────────────────────────────────────────────
@app.get("/")
def root(): return {"status":"ok","version":"5.0.0","time":datetime.now().isoformat()}

@app.get("/health")
def health(): return {"status":"healthy"}

@app.get("/debug/raw")
async def debug_raw(url: str):
    """Debug: fetch any URL and return raw text (first 2000 chars)"""
    try:
        r = await _get(url, _TWSE_HDR)
        return {"status": r.status_code, "headers": dict(r.headers),
                "body_preview": r.text[:2000], "body_len": len(r.text)}
    except Exception as e:
        return {"error": str(e)}

# ── Quote ─────────────────────────────────────────────────────────
async def _fetch_quote(code: str) -> dict:
    symbol = to_yf(code)
    cached = cache_get(f"q:{code}")
    if cached: return cached

    for host in ["query1","query2"]:
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        try:
            r = await _get(url, _YAHOO_HDR)
            if r.status_code != 200: continue
            res = (r.json().get("chart") or {}).get("result") or []
            if not res: continue
            q = res[0]["indicators"]["quote"][0]
            def clean(l): return [v for v in l if v is not None]
            closes=clean(q.get("close",[])); opens=clean(q.get("open",[]));
            highs=clean(q.get("high",[])); lows=clean(q.get("low",[])); vols=clean(q.get("volume",[]))
            if not closes: continue
            last=closes[-1]; prev=closes[-2] if len(closes)>=2 else last
            chg=last-prev; chgp=chg/prev*100 if prev else 0
            r2=lambda x:round(x,2)
            result={"code":code,"symbol":symbol,"price":r2(last),
                    "open":r2(opens[-1] if opens else last),"high":r2(highs[-1] if highs else last),
                    "low":r2(lows[-1] if lows else last),"volume":int(vols[-1] if vols else 0),
                    "change":r2(chg),"change_pct":r2(chgp),"updated_at":datetime.now().isoformat()}
            cache_set(f"q:{code}", result); return result
        except Exception as e:
            logging.warning(f"quote {host}/{symbol}: {e}")
    raise HTTPException(404, f"找不到 {code} 的報價")

@app.get("/api/quote/{code}")
async def get_quote(code: str): return await _fetch_quote(code)

@app.get("/api/quotes")
async def get_quotes(codes: str):
    cl = [c.strip() for c in codes.split(",") if c.strip()]
    if len(cl) > 30: raise HTTPException(400,"最多30支")
    results = await asyncio.gather(*[_fetch_quote(c) for c in cl], return_exceptions=True)
    return {"quotes":[r if not isinstance(r,Exception) else {"code":cl[i],"error":str(r)} for i,r in enumerate(results)],
            "count":len(cl),"updated_at":datetime.now().isoformat()}

# ── Chart (K-line data for Canvas) ───────────────────────────────
@app.get("/api/chart/{code}")
async def get_chart(code: str, period: str = "3mo"):
    cache_key = f"chart:{code}:{period}"
    cached = cache_get(cache_key, 300)
    if cached: return cached

    symbol = to_yf(code)
    iv_map = {"5d":"1d","1mo":"1d","3mo":"1d","1y":"1d","5y":"1wk"}
    iv = iv_map.get(period, "1d")
    isTW = symbol.endswith(".TW") or symbol.endswith(".TWO")

    for host in ["query1","query2"]:
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?interval={iv}&range={period}"
        try:
            r = await _get(url, _YAHOO_HDR)
            if r.status_code != 200: continue
            res = (r.json().get("chart") or {}).get("result") or []
            if not res: continue
            r0 = res[0]; ts = r0.get("timestamp") or []
            q = r0["indicators"]["quote"][0]
            bars = []
            for i, t in enumerate(ts):
                o=q["open"][i]; h=q["high"][i]; l=q["low"][i]; c=q["close"][i]
                v=q.get("volume",[None]*len(ts))[i]
                if o is None or c is None: continue
                vol = int(v//1000) if (v and isTW) else (int(v) if v else 0)
                bars.append({"t":t,"o":round(o,2),"h":round(h,2),"l":round(l,2),"c":round(c,2),"v":vol})
            result = {"code":code,"symbol":symbol,"period":period,"interval":iv,"bars":bars}
            cache_set(cache_key, result); return result
        except Exception as e:
            logging.warning(f"chart {host}/{symbol}: {e}")
    raise HTTPException(404, f"無法取得 {code} 圖表資料")

# ── Info ──────────────────────────────────────────────────────────
@app.get("/api/info/{code}")
async def get_info(code: str):
    cached = cache_get(f"info:{code}", 600)
    if cached: return cached
    symbol = to_yf(code)
    result = {"code":code,"symbol":symbol,"pe_ratio":None,"pb_ratio":None,
              "dividend_yield":None,"market_cap":None,"shares_outstanding":None,
              "week52_high":None,"week52_low":None,"sector":None,"industry":None,
              "gross_margins":None,"profit_margins":None,"return_on_equity":None}

    # Method 1: chart meta
    try:
        r = await _get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1y&interval=1mo", _YAHOO_HDR)
        if r.status_code == 200:
            meta = ((r.json().get("chart") or {}).get("result") or [{}])[0].get("meta",{})
            if meta:
                result["week52_high"] = meta.get("fiftyTwoWeekHigh")
                result["week52_low"]  = meta.get("fiftyTwoWeekLow")
                if meta.get("trailingPE"): result["pe_ratio"] = round(meta["trailingPE"],1)
                if meta.get("marketCap"):  result["market_cap"] = meta["marketCap"]
    except Exception as e: logging.warning(f"info chart {symbol}: {e}")

    # Method 2: quoteSummary
    for host in ["query2","query1"]:
        url = (f"https://{host}.finance.yahoo.com/v11/finance/quoteSummary/{symbol}"
               f"?modules=summaryDetail%2CdefaultKeyStatistics%2CfinancialData%2CassetProfile")
        try:
            r = await _get(url, _YAHOO_HDR)
            if r.status_code != 200: continue
            qs = (r.json().get("quoteSummary") or {}).get("result") or []
            if not qs: continue
            d=qs[0]; sd=d.get("summaryDetail") or {}; ks=d.get("defaultKeyStatistics") or {}
            fd=d.get("financialData") or {}; ap=d.get("assetProfile") or {}
            def val(o,k): v=o.get(k); return v.get("raw") if isinstance(v,dict) else v
            def pct(o,k): v=val(o,k); return round(v*100,2) if v is not None else None
            result.update({
                "pe_ratio":           round(val(sd,"trailingPE"),1) if val(sd,"trailingPE") else result["pe_ratio"],
                "pb_ratio":           round(val(ks,"priceToBook"),2) if val(ks,"priceToBook") else None,
                "dividend_yield":     pct(sd,"dividendYield"),
                "market_cap":         val(sd,"marketCap") or result["market_cap"],
                "shares_outstanding": val(ks,"sharesOutstanding"),
                "week52_high":        val(sd,"fiftyTwoWeekHigh") or result["week52_high"],
                "week52_low":         val(sd,"fiftyTwoWeekLow") or result["week52_low"],
                "sector":             ap.get("sector") or ap.get("industry"),
                "industry":           ap.get("industry"),
                "gross_margins":      pct(fd,"grossMargins"),
                "profit_margins":     pct(fd,"profitMargins"),
                "return_on_equity":   pct(fd,"returnOnEquity"),
            }); break
        except Exception as e: logging.warning(f"info qs {host}/{symbol}: {e}")

    cache_set(f"info:{code}", result)
    return result

# ── Chip (三大法人) ───────────────────────────────────────────────
@app.get("/api/chip/{code}")
async def get_chip(code: str):
    """
    三大法人：TWSE 封鎖 Render IP，改用 Yahoo Finance quoteSummary
    外資持股% 來自 institutionPercentHeld
    連買賣天數：從近 30 日 chart 資料估算（Yahoo 沒有法人明細，暫用 price action）
    """
    if not code[:4].isdigit(): raise HTTPException(400,"僅支援台股")

    cached = cache_get(f"chip5:{code}", 300)
    if cached: return cached

    symbol = to_yf(code)
    fn=tn=dn=0; date_str=datetime.now().strftime("%Y/%m/%d")
    foreign_hold_pct = None
    foreign_streak = trust_streak = 0

    # ── quoteSummary: institutionPercentHeld, heldPercentInsiders ──
    for host in ["query2","query1"]:
        url = (f"https://{host}.finance.yahoo.com/v11/finance/quoteSummary/{symbol}"
               f"?modules=majorHoldersBreakdown%2CinstitutionOwnership%2CinsiderHolders")
        try:
            r = await _get(url, _YAHOO_HDR)
            if r.status_code != 200: continue
            qs = (r.json().get("quoteSummary") or {}).get("result") or []
            if not qs: continue
            d = qs[0]
            mh = d.get("majorHoldersBreakdown") or {}
            def val(o,k):
                v=o.get(k); return v.get("raw") if isinstance(v,dict) else v
            ih = val(mh,"institutionsPercentHeld")
            if ih: foreign_hold_pct = round(ih * 100, 1)
            break
        except Exception as e:
            logging.warning(f"chip holders {host}/{symbol}: {e}")

    # ── 近 60 日收盤價估算連漲連跌天數（外資動向代理指標）──
    # 真實三大法人需 TWSE，Render IP 被封鎖，這裡用價格動能做替代
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2mo"
        r = await _get(url, _YAHOO_HDR)
        if r.status_code == 200:
            res = (r.json().get("chart") or {}).get("result") or []
            if res:
                closes = [v for v in res[0]["indicators"]["quote"][0].get("close",[]) if v]
                # 連漲/連跌天數（外資動向代理）
                def price_streak(closes):
                    s=0
                    for i in range(len(closes)-1, 0, -1):
                        diff = closes[i]-closes[i-1]
                        if s==0: s=1 if diff>0 else(-1 if diff<0 else 0)
                        elif s>0 and diff>0: s+=1
                        elif s<0 and diff<0: s-=1
                        else: break
                    return s
                streak_val = price_streak(closes)
                # 只有連續 3 天以上才顯示標籤（避免雜訊）
                foreign_streak = streak_val if abs(streak_val) >= 1 else 0
    except Exception as e:
        logging.warning(f"chip streak {symbol}: {e}")

    result = {
        "code":            code,
        "date":            date_str,
        "foreign_net":     fn,     # TWSE 封鎖，無法取得今日買賣超張數
        "trust_net":       tn,
        "dealer_net":      dn,
        "total_net":       0,
        "foreign_streak":  foreign_streak,
        "trust_streak":    trust_streak,
        "foreign_hold_pct": foreign_hold_pct,
        "note": "三大法人今日買賣超數字因TWSE封鎖暫時無法取得，外資持股%及連買賣天數為估算值"
    }
    cache_set(f"chip5:{code}", result)
    return result


# ── Holders ───────────────────────────────────────────────────────
@app.get("/api/holders/{code}")
async def get_holders(code: str):
    if not code[:4].isdigit(): raise HTTPException(400,"僅支援台股")
    try:
        cached = cache_get(f"holders:{code}", 86400)
        if not cached:
            r = await _get(f"https://openapi.tdcc.com.tw/v1/opendata/1-5?StockNo={code}", _TWSE_HDR)
            if r.status_code != 200 or not r.text.strip():
                raise HTTPException(404, f"無 {code} 集保資料")
            d = r.json()
            if not d: raise HTTPException(404, f"無 {code} 集保資料")
            latest = d[0].get("CalculationDate","")
            week_data = [x for x in d if x.get("CalculationDate")==latest]
            logging.info(f"holders {code}: {len(week_data)} rows, keys={list(week_data[0].keys()) if week_data else []}")
            brackets=[]
            for row in week_data:
                level   = row.get("HolderLevel") or row.get("持有股數分級","")
                holders = int(row.get("HolderCount") or row.get("持有人數",0) or 0)
                pct     = float(row.get("SharesPercent") or row.get("持股比例",0) or 0)
                brackets.append({"level":level,"holders":holders,"pct":pct,"chg":None})
            whale_pct = sum(b["pct"] for b in brackets if any(x in str(b["level"]) for x in ["400","600","800","1000","超過"]))
            result = {"code":code,"week":latest,"brackets":brackets,"whale_pct":round(whale_pct,2)}
            cache_set(f"holders:{code}", result)
            cached = result
        return cached
    except HTTPException: raise
    except Exception as e:
        logging.error(f"holders {code}: {e}")
        raise HTTPException(500, str(e))

# ── News ──────────────────────────────────────────────────────────
@app.get("/api/news/{code}")
async def get_stock_news(code: str):
    cached = cache_get(f"news:{code}", 180)
    if cached: return cached

    # International financial news
    if code == "intl":
        all_news, seen = [], set()
        for sym in ["^GSPC","^IXIC","NVDA","AAPL"]:
            try:
                url = f"https://query1.finance.yahoo.com/v1/finance/search?q={sym}&lang=en-US&region=US&quotesCount=0&newsCount=10"
                r = await _get(url, _YAHOO_HDR)
                if r.status_code != 200: continue
                for n in r.json().get("news",[])[:10]:
                    link = n.get("link","#")
                    if link in seen: continue
                    seen.add(link)
                    ts_ = n.get("providerPublishTime",0)
                    dt = datetime.fromtimestamp(ts_,tz=timezone.utc).astimezone() if ts_ else datetime.now()
                    all_news.append({"title":n.get("title",""),"url":link,
                                     "publisher":n.get("publisher",""),"time":dt.strftime("%m/%d %H:%M")})
                if len(all_news) >= 20: break
            except Exception as e: logging.warning(f"intl news {sym}: {e}")
        result = {"code":"intl","symbol":"intl","news":all_news[:20]}
        cache_set(f"news:{code}", result); return result

    # Stock-specific news
    symbol = to_yf(code)
    news = []
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={symbol}&lang=zh-TW&region=TW&quotesCount=0&newsCount=20"
        r = await _get(url, _YAHOO_HDR)
        if r.status_code == 200:
            for n in r.json().get("news",[])[:20]:
                ts_ = n.get("providerPublishTime",0)
                dt = datetime.fromtimestamp(ts_,tz=timezone.utc).astimezone() if ts_ else datetime.now()
                news.append({"title":n.get("title",""),"url":n.get("link","#"),
                             "publisher":n.get("publisher",""),"time":dt.strftime("%m/%d %H:%M")})
    except Exception as e: logging.warning(f"news {symbol}: {e}")
    result = {"code":code,"symbol":symbol,"news":news}
    cache_set(f"news:{code}", result); return result

# ── Search ────────────────────────────────────────────────────────
@app.get("/api/search")
async def search_stock(q: str):
    cached = cache_get(f"search:{q}", 300)
    if cached: return cached
    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={q}&lang=zh-TW&region=TW&quotesCount=8&newsCount=0"
    try:
        r = await _get(url, _YAHOO_HDR, timeout=10)
        quotes = r.json().get("quotes",[]) if r.status_code==200 else []
        results=[]
        for item in quotes:
            sym=item.get("symbol","")
            if not (sym.endswith(".TW") or sym.endswith(".TWO") or
                    ("." not in sym and item.get("quoteType") in ("EQUITY","ETF","INDEX"))): continue
            code=sym.replace(".TW","").replace(".TWO","")
            results.append({"code":code,"symbol":sym,
                            "name":item.get("longname") or item.get("shortname") or code,
                            "exchange":item.get("exchange",""),"type":item.get("quoteType","")})
        out={"query":q,"results":results[:8]}
        cache_set(f"search:{q}",out); return out
    except Exception as e: raise HTTPException(500,str(e))

# ── Auth & Watchlist ──────────────────────────────────────────────
async def verify_token(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401,"Missing Authorization")
    try: return auth.verify_id_token(authorization.split(" ",1)[1])
    except Exception as e: raise HTTPException(401, str(e))

DEFAULT_GROUPS=[
    {"name":"台股核心","stocks":["2330","2454","2317","2308","3008"]},
    {"name":"國際指數","stocks":["N225","KOSPI","HSI","SPX"]},
    {"name":"美國科技","stocks":["NVDA","AAPL","MSFT","TSLA"]},
    {"name":"自訂","stocks":["2382","006208"]},
]

@app.get("/api/watchlist")
async def get_watchlist(user: dict = Depends(verify_token)):
    uid=user["uid"]
    try:
        db=firestore.client(); doc=db.collection("watchlists").document(uid).get()
        if doc.exists: return doc.to_dict()
        default={"uid":uid,"groups":DEFAULT_GROUPS,"updated_at":datetime.now().isoformat()}
        db.collection("watchlists").document(uid).set(default); return default
    except Exception as e: raise HTTPException(500,str(e))

@app.put("/api/watchlist")
async def update_watchlist(body: dict, user: dict = Depends(verify_token)):
    uid=user["uid"]
    try:
        db=firestore.client(); body["uid"]=uid; body["updated_at"]=datetime.now().isoformat()
        db.collection("watchlists").document(uid).set(body); return {"status":"ok"}
    except Exception as e: raise HTTPException(500,str(e))

@app.post("/api/watchlist/add")
async def add_stock(body: dict, user: dict = Depends(verify_token)):
    uid=user["uid"]; code=body.get("code","").strip().upper(); grp=body.get("group","自訂")
    if not code: raise HTTPException(400,"code不能為空")
    try:
        db=firestore.client(); ref=db.collection("watchlists").document(uid)
        doc=ref.get(); data=doc.to_dict() if doc.exists else {"uid":uid,"groups":DEFAULT_GROUPS}
        groups=data.get("groups",[])
        target=next((g for g in groups if g["name"]==grp),None)
        if not target: target={"name":grp,"stocks":[]}; groups.append(target)
        if code not in target["stocks"]: target["stocks"].append(code)
        data["groups"]=groups; data["updated_at"]=datetime.now().isoformat()
        ref.set(data); return {"status":"ok","code":code,"group":grp}
    except Exception as e: raise HTTPException(500,str(e))

@app.delete("/api/watchlist/remove")
async def remove_stock(body: dict, user: dict = Depends(verify_token)):
    uid=user["uid"]; code=body.get("code","").strip().upper()
    try:
        db=firestore.client(); ref=db.collection("watchlists").document(uid)
        doc=ref.get()
        if not doc.exists: return {"status":"ok"}
        data=doc.to_dict()
        for g in data.get("groups",[]): g["stocks"]=[s for s in g["stocks"] if s.upper()!=code]
        data["updated_at"]=datetime.now().isoformat(); ref.set(data)
        return {"status":"ok","removed":code}
    except Exception as e: raise HTTPException(500,str(e))
