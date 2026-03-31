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
    if not code[:4].isdigit(): raise HTTPException(400,"僅支援台股")

    fn=tn=dn=fs=ts=0; date_str=""

    # ── Today: try 3 different TWSE endpoints ──
    today_endpoints = [
        ("openapi", "https://openapi.twse.com.tw/v1/fund/T86"),
        ("rwd",     f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"),
        ("zh",      f"https://www.twse.com.tw/zh/fund/T86?response=json&selectType=ALL"),
    ]
    for ep_name, ep_url in today_endpoints:
        try:
            ck = f"chip_t:{code}:{ep_name}"
            cached = cache_get(ck, 300)
            if not cached:
                r = await _get(ep_url, _TWSE_HDR)
                body = r.text.strip()
                logging.info(f"chip {ep_name}: status={r.status_code} len={len(body)} preview={body[:100]!r}")
                if r.status_code != 200 or not body or body[0] not in '[{': continue

                raw = r.json()
                rows = raw if isinstance(raw, list) else raw.get("data") or raw.get("items") or []
                if not rows: continue
                logging.info(f"chip {ep_name}: {len(rows)} rows, keys={list(rows[0].keys())[:8]}")

                row = None
                for x in rows:
                    for key in ["Code","股票代號","code","Symbol","證券代號"]:
                        if str(x.get(key,"")).strip() == code:
                            row = x; break
                    if row: break

                if not row:
                    logging.warning(f"chip {ep_name}: code {code} not in {len(rows)} rows")
                    continue

                logging.info(f"chip {ep_name}: row={row}")

                def fv(*keys):
                    for k in keys:
                        v = row.get(k)
                        if v is not None: return v
                    return "0"

                # Net buy directly (some endpoints have it)
                fn_net = fv("Foreign_Investor_Net_Buy_Sell","外陸資淨買賣超股數","foreignNet","外資淨買賣超股數")
                tn_net = fv("Investment_Trust_Net_Buy_Sell","投信淨買賣超股數","trustNet")
                dn_net = fv("Dealer_Net_Buy_Sell","自營商淨買賣超股數","dealerNet","自營商淨買賣超")

                # Or compute from buy - sell
                fb = fv("Foreign_Investor_Buy","外陸資買進股數","foreignBuy")
                fs_ = fv("Foreign_Investor_Sell","外陸資賣出股數","foreignSell")
                tb = fv("Investment_Trust_Buy","投信買進股數","trustBuy")
                ts_ = fv("Investment_Trust_Sell","投信賣出股數","trustSell")
                db = fv("Dealer_Buy","自營商買進股數","dealerBuy")
                ds = fv("Dealer_Sell","自營商賣出股數","dealerSell")

                fn_calc = _pn(fb) - _pn(fs_)
                tn_calc = _pn(tb) - _pn(ts_)
                dn_calc = _pn(db) - _pn(ds)

                fn = _pn(fn_net) if _pn(fn_net) else fn_calc
                tn = _pn(tn_net) if _pn(tn_net) else tn_calc
                dn = _pn(dn_net) if _pn(dn_net) else dn_calc
                date_str = str(row.get("Date","") or row.get("日期",""))

                cached = {"fn":fn,"tn":tn,"dn":dn,"date":date_str}
                cache_set(ck, cached)

            if cached:
                fn=cached["fn"]; tn=cached["tn"]; dn=cached["dn"]; date_str=cached["date"]
                break
        except Exception as e:
            logging.warning(f"chip today {ep_name}: {e}")

    # ── Historical streak ──
    hist_endpoints = [
        ("openapi_hist", f"https://openapi.twse.com.tw/v1/fund/TWT38U?StockNo={code}"),
        ("rwd_hist",     f"https://www.twse.com.tw/rwd/zh/fund/TWT38U?response=json&stockNo={code}"),
    ]
    for ep_name, ep_url in hist_endpoints:
        try:
            ck = f"chip_h:{code}:{ep_name}"
            cached_h = cache_get(ck, 3600)
            if not cached_h:
                r = await _get(ep_url, _TWSE_HDR)
                body = r.text.strip()
                logging.info(f"hist {ep_name}: status={r.status_code} len={len(body)}")
                if r.status_code != 200 or not body or body[0] not in '[{': continue
                raw = r.json()
                rows = raw if isinstance(raw, list) else raw.get("data") or []
                if not rows: continue
                logging.info(f"hist {ep_name}: {len(rows)} rows, keys={list(rows[0].keys())[:8]}")

                fk = list(rows[0].keys())
                def auto_key(rows, *patterns):
                    fk = list(rows[0].keys()) if rows else []
                    for pat in patterns:
                        for k in fk:
                            if all(p in k.lower() for p in pat.lower().split()): return k
                    return patterns[0]

                buy_f  = auto_key(rows, "foreign buy", "外陸資買進", "外資買進")
                sell_f = auto_key(rows, "foreign sell", "外陸資賣出", "外資賣出")
                buy_t  = auto_key(rows, "trust buy", "投信買進")
                sell_t = auto_key(rows, "trust sell", "投信賣出")

                def streak(rows, bk, sk):
                    s=0
                    for row in reversed(rows[-30:]):
                        try: v=_pn(row.get(bk,0))-_pn(row.get(sk,0))
                        except: break
                        if s==0: s=1 if v>0 else(-1 if v<0 else 0)
                        elif s>0 and v>0: s+=1
                        elif s<0 and v<0: s-=1
                        else: break
                    return s

                fs = streak(rows, buy_f, sell_f)
                ts = streak(rows, buy_t, sell_t)
                cached_h = {"fs":fs,"ts":ts}
                cache_set(ck, cached_h)

            if cached_h: fs=cached_h["fs"]; ts=cached_h["ts"]; break
        except Exception as e:
            logging.warning(f"chip hist {ep_name}: {e}")

    return {"code":code,"date":date_str,
            "foreign_net":fn,"trust_net":tn,"dealer_net":dn,
            "total_net":fn+tn+dn,"foreign_streak":fs,"trust_streak":ts}

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
