"""
股票儀表板後端 API v4
- httpx proxy=None 直連 Yahoo Finance + TWSE OpenAPI
"""
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import asyncio, time, os, json, logging
from typing import Optional
import httpx
import firebase_admin
from firebase_admin import credentials, auth, firestore

logging.basicConfig(level=logging.INFO)

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

app = FastAPI(title="Stock Dashboard API", version="4.0.0")
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
}
_TWSE_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.twse.com.tw/",
    "Origin": "https://www.twse.com.tw",
}

async def _fetch_yahoo(url: str, ttl=60):
    key = f"yh:{url}"
    c = cache_get(key, ttl)
    if c: return c
    transport = httpx.AsyncHTTPTransport(proxy=None)
    async with httpx.AsyncClient(transport=transport, timeout=20,
                                  follow_redirects=True) as cl:
        r = await cl.get(url, headers=_HDR)
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"Yahoo {r.status_code}")
    d = r.json()
    cache_set(key, d)
    return d

async def _fetch_twse(url: str, ttl=120):
    key = f"tw:{url}"
    c = cache_get(key, ttl)
    if c: return c
    transport = httpx.AsyncHTTPTransport(proxy=None)
    async with httpx.AsyncClient(transport=transport, timeout=20,
                                  follow_redirects=True, max_redirects=10) as cl:
        r = await cl.get(url, headers=_TWSE_HDR)
    logging.info(f"TWSE {url[-60:]} => {r.status_code}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"TWSE {r.status_code}")
    d = r.json()
    cache_set(key, d)
    return d

# ── Health ────────────────────────────────────────────────────────
@app.get("/")
def root(): return {"status":"ok","version":"4.0.0","time":datetime.now().isoformat()}

@app.get("/health")
def health(): return {"status":"healthy"}

# ── Debug: show first row of TWSE T86 ────────────────────────────
@app.get("/debug/twse")
async def debug_twse():
    """Show actual field names from TWSE OpenAPI"""
    try:
        d = await _fetch_twse("https://openapi.twse.com.tw/v1/fund/T86")
        first = d[0] if d else {}
        twse_row = next((x for x in d if x.get("Code","") == "2330"), first)
        return {"total": len(d), "fields": list(first.keys()), "row_2330": twse_row}
    except Exception as e:
        return {"error": str(e)}

# ── Quote ─────────────────────────────────────────────────────────
async def _fetch_quote(code: str) -> dict:
    symbol = to_yf(code)
    cached = cache_get(f"q:{code}")
    if cached: return cached
    transport = httpx.AsyncHTTPTransport(proxy=None)
    for host in ["query1","query2"]:
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        try:
            async with httpx.AsyncClient(transport=transport, timeout=20,
                                          follow_redirects=True) as cl:
                r = await cl.get(url, headers=_HDR)
            if r.status_code != 200: continue
            d = r.json()
            res = (d.get("chart") or {}).get("result") or []
            if not res: continue
            q = res[0]["indicators"]["quote"][0]
            def clean(l): return [v for v in l if v is not None]
            closes = clean(q.get("close",[])); opens=clean(q.get("open",[])); highs=clean(q.get("high",[])); lows=clean(q.get("low",[])); vols=clean(q.get("volume",[]))
            if not closes: continue
            last=closes[-1]; prev=closes[-2] if len(closes)>=2 else last
            chg=last-prev; chgp=chg/prev*100 if prev else 0
            r2=lambda x:round(x,2)
            result={"code":code,"symbol":symbol,"price":r2(last),
                    "open":r2(opens[-1] if opens else last),"high":r2(highs[-1] if highs else last),
                    "low":r2(lows[-1] if lows else last),"volume":int(vols[-1] if vols else 0),
                    "change":r2(chg),"change_pct":r2(chgp),"updated_at":datetime.now().isoformat()}
            cache_set(f"q:{code}",result); return result
        except Exception as e:
            logging.warning(f"quote {host}/{symbol}: {e}"); continue
    raise HTTPException(404, f"找不到 {code} 的報價")

@app.get("/api/quote/{code}")
async def get_quote(code: str): return await _fetch_quote(code)

@app.get("/api/quotes")
async def get_quotes(codes: str):
    cl=[c.strip() for c in codes.split(",") if c.strip()]
    if len(cl)>30: raise HTTPException(400,"最多30支")
    results=await asyncio.gather(*[_fetch_quote(c) for c in cl],return_exceptions=True)
    return {"quotes":[r if not isinstance(r,Exception) else {"code":cl[i],"error":str(r)} for i,r in enumerate(results)],
            "count":len(cl),"updated_at":datetime.now().isoformat()}

# ── Info ──────────────────────────────────────────────────────────
@app.get("/api/info/{code}")
async def get_info(code: str):
    cached = cache_get(f"info:{code}", 600)
    if cached: return cached
    symbol = to_yf(code)
    transport = httpx.AsyncHTTPTransport(proxy=None)

    result = {"code":code,"symbol":symbol,"pe_ratio":None,"pb_ratio":None,
              "dividend_yield":None,"market_cap":None,"shares_outstanding":None,
              "week52_high":None,"week52_low":None,"sector":None,"industry":None,
              "gross_margins":None,"profit_margins":None,"return_on_equity":None}

    # Method 1: chart meta (always works)
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1y&interval=1mo"
        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(proxy=None),
                                      timeout=15, follow_redirects=True) as cl:
            r = await cl.get(url, headers=_HDR)
        if r.status_code == 200:
            meta = ((r.json().get("chart") or {}).get("result") or [{}])[0].get("meta",{})
            if meta:
                result["week52_high"] = meta.get("fiftyTwoWeekHigh")
                result["week52_low"]  = meta.get("fiftyTwoWeekLow")
                if meta.get("trailingPE"): result["pe_ratio"] = round(meta["trailingPE"],1)
                if meta.get("marketCap"):  result["market_cap"] = meta["marketCap"]
                logging.info(f"info chart OK {symbol}")
    except Exception as e:
        logging.warning(f"info chart {symbol}: {e}")

    # Method 2: quoteSummary (more fields, may fail)
    for host in ["query2","query1"]:
        url = (f"https://{host}.finance.yahoo.com/v11/finance/quoteSummary/{symbol}"
               f"?modules=summaryDetail%2CdefaultKeyStatistics%2CfinancialData%2CassetProfile")
        try:
            async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(proxy=None),
                                          timeout=20, follow_redirects=True) as cl:
                r = await cl.get(url, headers=_HDR)
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
            })
            logging.info(f"info quoteSummary OK {symbol}")
            break
        except Exception as e:
            logging.warning(f"info qS {host}/{symbol}: {e}"); continue

    cache_set(f"info:{code}", result)
    return result

# ── Chip ──────────────────────────────────────────────────────────
@app.get("/api/chip/{code}")
async def get_chip(code: str):
    if not code[:4].isdigit(): raise HTTPException(400,"僅支援台股")

    foreign_net=trust_net=dealer_net=foreign_streak=trust_streak=0
    date_str=""

    def pn(s):
        try: return int(str(s).replace(",","").replace(" ","").replace("\u00a0",""))
        except: return 0

    # Today's institutional data
    try:
        cached = cache_get(f"chip_t:{code}", 300)
        if not cached:
            # Try TWSE OpenAPI first
            d = await _fetch_twse("https://openapi.twse.com.tw/v1/fund/T86")
            logging.info(f"T86 total rows: {len(d)}, first keys: {list(d[0].keys()) if d else []}")
            row = next((x for x in d if str(x.get("Code","")).strip() == code or
                                        str(x.get("股票代號","")).strip() == code or
                                        str(x.get("證券代號","")).strip() == code), None)
            if row:
                logging.info(f"T86 row keys: {list(row.keys())}")
                # Try multiple possible field name patterns
                # Pattern A: English keys
                fn_a = pn(row.get("Foreign_Investor_Buy",0)) - pn(row.get("Foreign_Investor_Sell",0))
                # Pattern B: Chinese keys  
                fn_b = pn(row.get("外陸資買進股數",0)) - pn(row.get("外陸資賣出股數",0))
                # Use whichever is non-zero
                foreign_net = fn_a if fn_a != 0 else fn_b
                tn_a = pn(row.get("Investment_Trust_Buy",0)) - pn(row.get("Investment_Trust_Sell",0))
                tn_b = pn(row.get("投信買進股數",0)) - pn(row.get("投信賣出股數",0))
                trust_net = tn_a if tn_a != 0 else tn_b
                dn_a = pn(row.get("Dealer_Buy",0)) - pn(row.get("Dealer_Sell",0))
                dn_b = pn(row.get("自營商買進股數",0)) - pn(row.get("自營商賣出股數",0))
                dealer_net = dn_a if dn_a != 0 else dn_b
                date_str = row.get("Date","") or row.get("日期","")
                cached = {"fn":foreign_net,"tn":trust_net,"dn":dealer_net,"date":date_str}
                cache_set(f"chip_t:{code}", cached)
        if cached:
            foreign_net=cached["fn"]; trust_net=cached["tn"]
            dealer_net=cached["dn"]; date_str=cached["date"]
    except Exception as e:
        logging.warning(f"chip today {code}: {e}")

    # Historical streak
    try:
        cached_h = cache_get(f"chip_h:{code}", 3600)
        if not cached_h:
            d = await _fetch_twse(f"https://openapi.twse.com.tw/v1/fund/TWT38U?StockNo={code}")
            logging.info(f"TWT38U {code}: {len(d)} rows, keys: {list(d[0].keys()) if d else []}")
            def streak(rows, bk, sk):
                s=0
                for row in reversed(rows[-30:]):
                    try: v=pn(row.get(bk,0))-pn(row.get(sk,0))
                    except: break
                    if s==0: s=1 if v>0 else(-1 if v<0 else 0)
                    elif s>0 and v>0: s+=1
                    elif s<0 and v<0: s-=1
                    else: break
                return s
            # Try both English and Chinese field names
            if d:
                fk = list(d[0].keys())
                logging.info(f"TWT38U fields: {fk}")
                buy_f  = "Foreign_Investor_Buy"  if "Foreign_Investor_Buy"  in fk else "外資及陸資買進股數"
                sell_f = "Foreign_Investor_Sell" if "Foreign_Investor_Sell" in fk else "外資及陸資賣出股數"
                buy_t  = "Investment_Trust_Buy"  if "Investment_Trust_Buy"  in fk else "投信買進股數"
                sell_t = "Investment_Trust_Sell" if "Investment_Trust_Sell" in fk else "投信賣出股數"
                foreign_streak = streak(d, buy_f, sell_f)
                trust_streak   = streak(d, buy_t, sell_t)
            cached_h = {"fs":foreign_streak,"ts":trust_streak}
            cache_set(f"chip_h:{code}", cached_h)
        if cached_h:
            foreign_streak=cached_h["fs"]; trust_streak=cached_h["ts"]
    except Exception as e:
        logging.warning(f"chip hist {code}: {e}")

    return {"code":code,"date":date_str,
            "foreign_net":foreign_net,"trust_net":trust_net,"dealer_net":dealer_net,
            "total_net":foreign_net+trust_net+dealer_net,
            "foreign_streak":foreign_streak,"trust_streak":trust_streak}

# ── Holders ───────────────────────────────────────────────────────
@app.get("/api/holders/{code}")
async def get_holders(code: str):
    if not code[:4].isdigit(): raise HTTPException(400,"僅支援台股")
    try:
        d = await _fetch_twse(
            f"https://openapi.tdcc.com.tw/v1/opendata/1-5?StockNo={code}",
            ttl=86400)
        if not d: raise HTTPException(404,f"無 {code} 集保資料")
        latest = d[0].get("CalculationDate","")
        week_data = [r for r in d if r.get("CalculationDate")==latest]
        logging.info(f"holders {code}: {len(week_data)} rows, keys: {list(week_data[0].keys()) if week_data else []}")
        brackets=[]
        for row in week_data:
            # field names may vary
            level   = row.get("HolderLevel") or row.get("持有股數分級","")
            holders = int(row.get("HolderCount") or row.get("持有人數",0) or 0)
            pct     = float(row.get("SharesPercent") or row.get("持股比例",0) or 0)
            brackets.append({"level":level,"holders":holders,"pct":pct,"chg":None})
        whale_pct = sum(b["pct"] for b in brackets
                        if any(x in str(b["level"]) for x in ["400","600","800","1000","超過"]))
        return {"code":code,"week":latest,"brackets":brackets,"whale_pct":round(whale_pct,2)}
    except HTTPException: raise
    except Exception as e:
        logging.error(f"holders {code}: {e}")
        raise HTTPException(500, str(e))

# ── Search ────────────────────────────────────────────────────────
@app.get("/api/search")
async def search_stock(q: str):
    cached = cache_get(f"search:{q}", 300)
    if cached: return cached
    transport = httpx.AsyncHTTPTransport(proxy=None)
    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={q}&lang=zh-TW&region=TW&quotesCount=8&newsCount=0"
    try:
        async with httpx.AsyncClient(transport=transport,timeout=10,follow_redirects=True) as cl:
            r = await cl.get(url, headers=_HDR)
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
    except Exception as e:
        raise HTTPException(500,str(e))

# ── Stock News ────────────────────────────────────────────────────
@app.get("/api/news/{code}")
async def get_stock_news(code: str):
    cached = cache_get(f"news:{code}", 300)
    if cached: return cached
    symbol = to_yf(code)
    transport = httpx.AsyncHTTPTransport(proxy=None)
    url = (f"https://query1.finance.yahoo.com/v1/finance/search"
           f"?q={symbol}&lang=zh-TW&region=TW&quotesCount=0&newsCount=8")
    try:
        async with httpx.AsyncClient(transport=transport,timeout=10,follow_redirects=True) as cl:
            r = await cl.get(url, headers=_HDR)
        raw_news = r.json().get("news",[]) if r.status_code==200 else []
        news=[]
        for n in raw_news[:8]:
            ts=n.get("providerPublishTime",0)
            dt=datetime.fromtimestamp(ts,tz=timezone.utc).astimezone() if ts else datetime.now()
            news.append({"title":n.get("title",""),"url":n.get("link","#"),
                         "publisher":n.get("publisher",""),"time":dt.strftime("%m/%d %H:%M")})
        result={"code":code,"symbol":symbol,"news":news}
        cache_set(f"news:{code}",result); return result
    except Exception as e:
        raise HTTPException(500,str(e))

# ── Auth ──────────────────────────────────────────────────────────
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
