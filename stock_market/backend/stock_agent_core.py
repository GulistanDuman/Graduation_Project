#!/usr/bin/env python3
"""
$50K Institutional Stock Research Agent — v4
=============================================
Changes from v3:
  - DEFAULT QUERY: NVIDIA stock analysis (hardcoded)
  - Report length: 20+ pages (7000-9000 words target)
  - Image planner: now targets 5 images (was 3)
  - KPI section: 3 rich markdown tables with formulas
  - Added: 1-Year Profit/Loss Expectation section (bull/base/bear scenarios)
  - Writer prompt: fan-out task structure — each section prepared as task, then integrated
  - Generated images distributed across relevant report sections
  - Clickable Tavily citations retained
  - Charts embedded as base64 retained
  - WebSocket + in-session cache retained
"""

import os, re, json, io, base64, asyncio, textwrap, hashlib, time, uuid
from typing import TypedDict, List, Optional, Dict, Any, Annotated, Literal
from datetime import datetime
from urllib.parse import urlparse, urljoin
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import requests
from bs4 import BeautifulSoup
try:
    from fake_useragent import UserAgent
except Exception:
    class UserAgent:
        @property
        def random(self) -> str:
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from tavily import TavilyClient
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

# ── optional PDF export ───────────────────────────────────────────────────
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# ── optional websocket ────────────────────────────────────────────────────
try:
    import websockets
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

load_dotenv()
logger.add("logs/agent.log", rotation="10 MB", level="INFO")
ua = UserAgent()

# ─────────────────────────────────────────────────────────────────────────
# DEFAULT QUERY — NVIDIA
# ─────────────────────────────────────────────────────────────────────────
DEFAULT_QUERY  = "NVIDIA stock deep dive analysis"
DEFAULT_TICKER = "NVDA"

# ─────────────────────────────────────────────────────────────────────────
# Environment Validator
# ─────────────────────────────────────────────────────────────────────────
REQUIRED_KEYS = {
    "OPENAI_API_KEY":   "OpenAI GPT-4o (writer + analysis + image planning)",
    "TAVILY_API_KEY":   "Tavily web search",
    "GOOGLE_API_KEY":   "Gemini image generation",
}
OPTIONAL_KEYS = {
    "WS_PORT": "WebSocket progress emitter (default 8765)",
}

def validate_environment() -> bool:
    missing = [k for k in REQUIRED_KEYS if not os.getenv(k)]
    if missing:
        logger.warning("─── Missing API keys ───────────────────────────────")
        for k in missing:
            logger.warning(f"  ✗  {k:30s}  ({REQUIRED_KEYS[k]})")
        logger.warning("────────────────────────────────────────────────────")
        logger.warning("Add them to your .env file. Agent will continue but affected nodes skipped.")
    else:
        logger.success("All required API keys found ✓")
    return len(missing) == 0

# ─────────────────────────────────────────────────────────────────────────
# In-session cache
# ─────────────────────────────────────────────────────────────────────────
_SESSION_CACHE: Dict[str, Any] = {}

def cache_key(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.md5(value.encode()).hexdigest()[:8]}"

def cached(key: str, fn):
    if key in _SESSION_CACHE:
        logger.debug(f"Cache hit: {key}")
        return _SESSION_CACHE[key]
    result = fn()
    _SESSION_CACHE[key] = result
    return result

# ─────────────────────────────────────────────────────────────────────────
# WebSocket progress emitter
# ─────────────────────────────────────────────────────────────────────────
_ws_clients: set = set()
_ws_loop: Optional[asyncio.AbstractEventLoop] = None

def emit_progress(node_name: str, status: str = "running", extra: str = ""):
    payload = json.dumps({"node": node_name, "status": status, "detail": extra,
                          "ts": datetime.utcnow().isoformat()})
    if WS_AVAILABLE and _ws_clients and _ws_loop:
        asyncio.run_coroutine_threadsafe(_broadcast(payload), _ws_loop)
    logger.info(f"[{status.upper()}] {node_name} {extra}")

async def _broadcast(msg: str):
    dead = set()
    for ws in _ws_clients:
        try:
            await ws.send(msg)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)

async def _ws_server():
    async def handler(ws, path=None):
        _ws_clients.add(ws)
        try:
            await ws.wait_closed()
        finally:
            _ws_clients.discard(ws)
    port = int(os.getenv("WS_PORT", "8765"))
    async with websockets.serve(handler, "localhost", port):
        logger.success(f"WebSocket server running on ws://localhost:{port}")
        await asyncio.Future()

def start_ws_server():
    if not WS_AVAILABLE:
        return
    global _ws_loop
    import threading
    _ws_loop = asyncio.new_event_loop()
    t = threading.Thread(target=_ws_loop.run_until_complete,
                         args=(_ws_server(),), daemon=True)
    t.start()

# ─────────────────────────────────────────────────────────────────────────
# Pydantic Models for Image Planning
# ─────────────────────────────────────────────────────────────────────────
class ImageSpec(BaseModel):
    placeholder: str = Field(..., description="e.g. [[IMAGE_1]]")
    filename: str = Field(..., description="Save under images/, e.g. revenue_breakdown.png")
    alt: str = Field(..., description="Alt text for accessibility")
    caption: str = Field(..., description="Short caption shown below image")
    prompt: str = Field(..., description="Detailed image generation prompt for Gemini")
    size: Literal["1024x1024", "1024x1536", "1536x1024"] = "1536x1024"

class GlobalImagePlan(BaseModel):
    md_with_placeholders: str = Field(..., description="Full markdown with [[IMAGE_N]] placeholders inserted at exact positions")
    images: List[ImageSpec] = Field(default_factory=list, description="Exactly 5 images, each with a detailed generation prompt")

# ─────────────────────────────────────────────────────────────────────────
# State Reducers
# ─────────────────────────────────────────────────────────────────────────
def keep_first(existing, new):   return existing if existing is not None else new
def merge_lists(existing, new):  return (existing or []) + (new or [])
def keep_latest(existing, new):  return new if new is not None else existing

# ─────────────────────────────────────────────────────────────────────────
# AgentState
# ─────────────────────────────────────────────────────────────────────────
class AgentState(TypedDict):
    query:              Annotated[str,            keep_first]
    ticker:             Annotated[Optional[str],  keep_first]
    parsed_query:       Annotated[Optional[str],  keep_first]

    tavily_citations:           Annotated[List[dict],   keep_latest]
    reuters_marketwatch_data:   Annotated[Optional[str],keep_latest]
    edgar_data:                 Annotated[Optional[str],keep_latest]
    stocktwits_data:            Annotated[Optional[str],keep_latest]

    yahoo_historical:   Annotated[Optional[str],  keep_latest]
    yahoo_live:         Annotated[Optional[str],  keep_latest]
    kpi_report:         Annotated[Optional[str],  keep_latest]
    charts:             Annotated[List[dict],      keep_latest]

    tavily_analysis:    Annotated[Optional[str],  keep_latest]
    reuters_analysis:   Annotated[Optional[str],  keep_latest]
    edgar_analysis:     Annotated[Optional[str],  keep_latest]
    stocktwits_analysis:Annotated[Optional[str],  keep_latest]
    kpi_analysis:       Annotated[Optional[str],  keep_latest]

    research_merged:    Annotated[Optional[str],  keep_latest]
    analyst_merged:     Annotated[Optional[str],  keep_latest]

    draft_report_markdown:  Annotated[Optional[str],   keep_latest]

    image_specs:            Annotated[Optional[List[dict]], keep_latest]
    md_with_placeholders:   Annotated[Optional[str],        keep_latest]

    final_report_markdown:  Annotated[Optional[str],   keep_latest]
    final_report_pdf:       Annotated[Optional[str],   keep_latest]

    risk_signal:            Annotated[Optional[str],   keep_latest]
    risk_reason:            Annotated[Optional[str],   keep_latest]

    confidence_score:       Annotated[Optional[float], keep_latest]
    section_scores:         Annotated[Optional[dict],  keep_latest]

    # NEW: 1-year profit/loss expectation
    profit_loss_expectation: Annotated[Optional[str],  keep_latest]

    retry_count:    Annotated[int,        keep_latest]
    max_retries:    int
    errors:         Annotated[List[str],  merge_lists]

# ─────────────────────────────────────────────────────────────────────────
# Rate-limit guard helper
# ─────────────────────────────────────────────────────────────────────────
def safe_get(url: str, headers: dict = None, timeout: int = 20) -> requests.Response:
    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=2, max=15),
           retry=retry_if_exception_type(requests.HTTPError))
    def _get():
        resp = requests.get(url, headers=headers or {"User-Agent": ua.random}, timeout=timeout)
        if resp.status_code == 429:
            raise requests.HTTPError("429 rate limited")
        resp.raise_for_status()
        return resp
    return _get()

# ─────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────
COMPANY_TICKER_MAP = {
    "nvidia": "NVDA",  "tesla":     "TSLA",  "apple":    "AAPL",
    "microsoft":"MSFT","amazon":    "AMZN",  "google":   "GOOGL",
    "meta":    "META", "netflix":   "NFLX",  "amd":      "AMD",
    "intel":   "INTC", "salesforce":"CRM",   "palantir": "PLTR",
    "coinbase":"COIN", "shopify":   "SHOP",  "alibaba":  "BABA",
}

def orchestrator_supervisor(state: AgentState) -> Dict[str, Any]:
    emit_progress("orchestrator", "running")
    original = state.get("query") or state.get("parsed_query") or ""
    lower    = original.lower()

    for comp, sym in COMPANY_TICKER_MAP.items():
        if re.search(rf"\b{re.escape(comp)}\b", lower):
            emit_progress("orchestrator", "done", f"ticker={sym}")
            return {"ticker": sym, "parsed_query": original}

    m = re.search(r'(?:ticker|symbol)\s*[:=]?\s*([A-Z]{1,5})\b', original, re.IGNORECASE)
    if m:
        ticker = m.group(1).upper()
        emit_progress("orchestrator", "done", f"ticker={ticker}")
        return {"ticker": ticker, "parsed_query": original}

    m = re.search(r'\b(?:stock|shares)\s*[:=]\s*([A-Z]{1,5})\b', original, re.IGNORECASE)
    if m:
        ticker = m.group(1).upper()
        emit_progress("orchestrator", "done", f"ticker={ticker}")
        return {"ticker": ticker, "parsed_query": original}

    stopwords = {"WITH", "YEAR", "ONE", "STOCK", "SHARE", "SHARES", "BUY", "SELL", "HOLD"}
    m = re.search(r'\b([A-Z]{1,5})\b', original)
    if m:
        ticker = m.group(1).upper()
        if ticker not in stopwords:
            emit_progress("orchestrator", "done", f"ticker={ticker} (guessed)")
            return {"ticker": ticker, "parsed_query": original}

    emit_progress("orchestrator", "done", f"ticker={DEFAULT_TICKER} (fallback)")
    return {"ticker": DEFAULT_TICKER, "parsed_query": original}

# ─────────────────────────────────────────────────────────────────────────
# Research Branch
# ─────────────────────────────────────────────────────────────────────────
def research_supervisor_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("research_supervisor", "running")
    return {}

# ─────────────────────────────────────────────────────────────────────────
# TAVILY NODE
# ─────────────────────────────────────────────────────────────────────────
TAVILY_ANALYSIS_SYSTEM = """You are a senior institutional equity analyst with 20+ years on Wall Street.
You have been given raw web search results from Tavily about a specific stock/company.

Your task is to produce a STRUCTURED, DEEP ANALYSIS of these results.

Follow this exact output format:

## 📰 News & Developments
- List the 5 most important recent news items with dates
- For each: what happened, why it matters, bullish/bearish impact

## 💡 Analyst & Market Consensus
- What are analysts saying? Any upgrades/downgrades?
- Price target changes? Rating changes?
- Consensus view: bullish / neutral / bearish with evidence

## 🏭 Business & Competitive Landscape
- Key business developments mentioned
- Competitive threats or advantages highlighted
- Any partnership, product launch, or strategic move noted

## 📊 Financial Highlights from News
- Any revenue, earnings, guidance numbers mentioned
- Beat/miss vs expectations
- Forward guidance commentary

## ⚠️ Risk Flags
- Regulatory risks, legal issues, macro headwinds
- Management changes, supply chain, geopolitical exposure
- Rate each risk: HIGH / MEDIUM / LOW

## 🎯 Overall Web Sentiment
- Aggregate sentiment from all sources: BULLISH / NEUTRAL / BEARISH
- Confidence in this assessment: HIGH / MEDIUM / LOW
- One-line investment thesis based purely on web data

RULES:
- Be specific with numbers, dates, and names — never vague
- Cite which source each claim comes from using [Source Name]
- If a source contradicts another, note the disagreement
- Do NOT hallucinate — only use information present in the provided data
- Keep analysis concise but dense with facts
- Use professional institutional language
"""

def tavily_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("tavily", "running")
    query = state.get("parsed_query") or state.get("query") or "stock analysis"
    ticker = state.get("ticker") or ""
    key = cache_key("tavily", query)

    def fetch():
        if not os.getenv("TAVILY_API_KEY"):
            return []
        client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

        results_all = []
        search_queries = [
            f"{ticker} stock analysis earnings forecast 2025",
            f"{ticker} latest news analyst rating price target",
            f"{ticker} financial results revenue growth outlook",
        ]

        seen_urls = set()
        for sq in search_queries:
            try:
                resp    = client.search(query=sq, max_results=5)
                results = resp.get("results", [])
                for r in results:
                    url = r["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    title   = r.get("title", "No title")
                    content = r.get("content", "")
                    score   = r.get("score", 0)

                    by_match = re.search(r'by\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', content)
                    author   = by_match.group(1) if by_match else urlparse(url).netloc.split('.')[0].capitalize()

                    published = r.get("published_date")
                    if published:
                        try:    year = datetime.fromisoformat(published.replace("Z","")).strftime("%Y-%m-%d")
                        except: year = "n.d."
                    else:
                        ym   = re.search(r'\b(20[0-2]\d)\b', content)
                        year = ym.group(1) if ym else "n.d."

                    results_all.append({
                        "citation_text": f"({author}, {year})",
                        "url":     url,
                        "title":   title,
                        "content": content[:800],
                        "score":   score,
                        "source":  urlparse(url).netloc.replace("www.",""),
                    })
            except Exception as e:
                logger.error(f"Tavily sub-query error: {e}")

        results_all.sort(key=lambda x: x.get("score", 0), reverse=True)
        return results_all[:10]

    try:
        citations = cached(key, fetch)
        emit_progress("tavily", "done", f"{len(citations)} results")
        return {"tavily_citations": citations}
    except Exception as e:
        logger.error(f"Tavily: {e}")
        emit_progress("tavily", "error", str(e))
        return {"tavily_citations": [], "errors": [f"Tavily: {e}"]}

def reuters_marketwatch_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("reuters_marketwatch", "running")
    ticker = state.get("ticker")
    if not ticker:
        return {"reuters_marketwatch_data": "No ticker.", "errors": ["R/M: no ticker"]}

    def fetch_reuters():
        url  = f"https://www.reuters.com/companies/{ticker}/"
        resp = safe_get(url)
        soup = BeautifulSoup(resp.text, "lxml")
        lines = []
        for article in soup.select("div.article-detail")[:5]:
            h3 = article.find("h3")
            a  = article.find("a", href=True)
            if h3 and a:
                lines.append(f"- [{h3.get_text(strip=True)}]({urljoin(url, a['href'])})")
        metrics = {}
        for row in soup.select("div.company-stats tr"):
            cells = row.find_all(["th","td"])
            if len(cells) == 2:
                metrics[cells[0].get_text(strip=True)] = cells[1].get_text(strip=True)
        headlines = "\n".join(lines) if lines else "No headlines."
        return f"**Reuters Headlines:**\n{headlines}\n\n**Key Metrics:** {json.dumps(metrics)}"

    def fetch_marketwatch():
        url  = f"https://www.marketwatch.com/investing/stock/{ticker.lower()}"
        resp = safe_get(url)
        soup = BeautifulSoup(resp.text, "lxml")
        lines = []
        for article in soup.select("div.article__content")[:5]:
            h3 = article.find("h3", class_="article__headline")
            a  = article.find("a",  class_="article__headline")
            if h3 and a:
                lines.append(f"- [{h3.get_text(strip=True)}]({urljoin(url, a['href'])})")
        metrics = {}
        for item in soup.select("li.kv__item"):
            label = item.select_one("small.primary")
            value = item.select_one("span.primary")
            if label and value:
                metrics[label.get_text(strip=True)] = value.get_text(strip=True)
        headlines = "\n".join(lines) if lines else "No headlines."
        return f"**MarketWatch Headlines:**\n{headlines}\n\n**Key Metrics:** {json.dumps(metrics)}"

    parts = []
    for name, fn in [("Reuters", fetch_reuters), ("MarketWatch", fetch_marketwatch)]:
        try:
            parts.append(cached(cache_key(name, ticker), fn))
        except Exception as e:
            parts.append(f"{name} error: {e}")
            logger.error(f"{name}: {e}")

    emit_progress("reuters_marketwatch", "done")
    return {"reuters_marketwatch_data": "\n\n".join(parts)}

def stocktwits_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("stocktwits", "running")
    ticker = state.get("ticker")
    if not ticker:
        return {"stocktwits_data": "No ticker."}
    try:
        resp = safe_get(f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json")
        data = resp.json()
        messages = data.get("messages", [])
        if not messages:
            return {"stocktwits_data": "No StockTwits messages."}
        lines = ["**StockTwits:**"]
        for msg in messages[:8]:
            sentiment = msg.get("entities",{}).get("sentiment",{})
            label     = sentiment.get("basic","") if sentiment else ""
            lines.append(f"- {msg['user']['username']} [{label}]: {msg['body']}")
        emit_progress("stocktwits", "done", f"{len(messages)} msgs")
        return {"stocktwits_data": "\n".join(lines)}
    except Exception as e:
        emit_progress("stocktwits", "error", str(e))
        return {"stocktwits_data": f"StockTwits error: {e}", "errors": [f"StockTwits: {e}"]}

# SEC EDGAR
SEC_HEADERS = {"User-Agent": "ResearchAgent research@agent.com",
               "Accept-Encoding": "gzip, deflate", "Host": "www.sec.gov"}
_TICKER_CIK_MAP = None

def load_ticker_cik_map():
    global _TICKER_CIK_MAP
    if _TICKER_CIK_MAP: return _TICKER_CIK_MAP
    resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS)
    resp.raise_for_status()
    _TICKER_CIK_MAP = {v["ticker"].upper(): str(v["cik_str"]).zfill(10)
                       for v in resp.json().values()}
    return _TICKER_CIK_MAP

def sec_edgar_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("edgar", "running")
    ticker = state.get("ticker")
    if not ticker:
        return {"edgar_data": "No ticker."}
    try:
        cik = load_ticker_cik_map().get(ticker.upper())
        if not cik:
            raise ValueError("CIK not found")
        url  = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=exclude&count=40"
        resp = safe_get(url, headers=SEC_HEADERS)
        soup = BeautifulSoup(resp.text, "lxml")
        filings, lines = [], [f"**SEC EDGAR Filings — {ticker}:**"]
        for row in soup.select("table.tableFile2 tr"):
            cells = row.find_all("td")
            if len(cells) >= 4 and cells[0].get_text(strip=True) in ["10-K","10-Q","8-K"]:
                link = "https://www.sec.gov" + cells[1].find("a")["href"] if cells[1].find("a") else ""
                lines.append(f"- [{cells[0].get_text(strip=True)} ({cells[3].get_text(strip=True)})]({link}): {cells[2].get_text(strip=True)}")
                filings.append(True)
            if len(filings) >= 6: break
        emit_progress("edgar", "done", f"{len(filings)} filings")
        return {"edgar_data": "\n".join(lines) if filings else f"No filings for {ticker}."}
    except Exception as e:
        emit_progress("edgar", "error", str(e))
        return {"edgar_data": f"EDGAR error: {e}", "errors": [f"EDGAR: {e}"]}

# ─────────────────────────────────────────────────────────────────────────
# GPT-4o analysis helpers
# ─────────────────────────────────────────────────────────────────────────
def _llm(max_tokens: int = 4096) -> ChatOpenAI:
    return ChatOpenAI(model="gpt-4o", temperature=0,
                      max_tokens=max_tokens,
                      api_key=os.getenv("OPENAI_API_KEY"))

def run_analysis(state_key: str, source_label: str, state: AgentState,
                 extra_instruction: str = "") -> Optional[str]:
    raw = state.get(state_key)
    if not raw: return None
    if isinstance(raw, list):
        if not raw: return None
        text = "\n".join(
            f"[{i.get('source','')}] {i.get('citation_text','')}: {i.get('title','')} — {i.get('content','')}"
            if isinstance(i, dict) else str(i) for i in raw
        )
    else:
        text = raw
    if not text.strip() or "No ticker" in text: return None
    try:
        prompt = (f"You are a senior equity analyst. Analyse this {source_label} data. "
                  f"Be factual, concise, and highlight bullish or bearish signals. "
                  f"{extra_instruction}\n\n{text[:5000]}")
        return _llm().invoke([HumanMessage(content=prompt)]).content.strip()
    except Exception as e:
        return f"[Analysis error: {e}]"

def tavily_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("tavily_analysis", "running")
    citations = state.get("tavily_citations", [])
    if not citations:
        return {"tavily_analysis": "No Tavily data available."}
    try:
        text_blocks = []
        for c in citations:
            block = (
                f"SOURCE: {c.get('source','unknown')} | TITLE: {c.get('title','')} | "
                f"DATE: {c.get('citation_text','')} | URL: {c.get('url','')}\n"
                f"CONTENT: {c.get('content','')}"
            )
            text_blocks.append(block)
        combined = "\n\n---\n\n".join(text_blocks)

        resp = _llm().invoke([
            SystemMessage(content=TAVILY_ANALYSIS_SYSTEM),
            HumanMessage(content=f"Analyse these web search results:\n\n{combined[:6000]}")
        ])
        emit_progress("tavily_analysis", "done")
        return {"tavily_analysis": resp.content.strip()}
    except Exception as e:
        emit_progress("tavily_analysis", "error", str(e))
        return {"tavily_analysis": f"[Tavily analysis error: {e}]"}

def reuters_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("reuters_analysis", "running")
    r = run_analysis("reuters_marketwatch_data", "Reuters & MarketWatch", state,
                     extra_instruction=(
                         "Focus on: 1) headline news and their market impact "
                         "2) key financial metrics scraped "
                         "3) any analyst commentary found "
                         "4) bullish/bearish bias of the coverage."
                     ))
    emit_progress("reuters_analysis", "done")
    return {"reuters_analysis": r}

def edgar_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("edgar_analysis", "running")
    r = run_analysis("edgar_data", "SEC EDGAR filings", state,
                     extra_instruction=(
                         "Highlight: 1) any recent 8-K risk events or material changes "
                         "2) 10-Q/10-K filing dates and what they likely contain "
                         "3) regulatory filing patterns — are they current or delayed? "
                         "4) any red flags in filing frequency."
                     ))
    emit_progress("edgar_analysis", "done")
    return {"edgar_analysis": r}

def stocktwits_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("stocktwits_analysis", "running")
    r = run_analysis("stocktwits_data", "StockTwits retail traders", state,
                     extra_instruction=(
                         "Analyse: 1) overall bull/bear ratio from sentiment labels "
                         "2) recurring themes and tickers mentioned "
                         "3) unusual conviction signals "
                         "4) contrarian signals (extreme sentiment)."
                     ))
    emit_progress("stocktwits_analysis", "done")
    return {"stocktwits_analysis": r}

def research_reducer_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("research_reducer", "running")
    parts = ["# FUNDAMENTAL RESEARCH SUMMARIES"]
    for label, key in [
        ("Tavily Web Research (Multi-Query)",  "tavily_analysis"),
        ("Reuters & MarketWatch",              "reuters_analysis"),
        ("StockTwits Retail Sentiment",        "stocktwits_analysis"),
        ("SEC EDGAR",                          "edgar_analysis"),
    ]:
        content = state.get(key)
        if content:
            parts.append(f"**{label}:**\n{content}")
    emit_progress("research_reducer", "done")
    return {"research_merged": "\n\n".join(parts)}

# ─────────────────────────────────────────────────────────────────────────
# Analyst Branch
# ─────────────────────────────────────────────────────────────────────────
def analyst_supervisor_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("analyst_supervisor", "running")
    return {}

def yahoo_finance_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("yahoo", "running")
    ticker = state.get("ticker")
    if not ticker:
        return {"yahoo_historical": "No ticker.", "yahoo_live": "N/A"}
    def fetch():
        stock = yf.Ticker(ticker)
        hist  = stock.history(period="5y")
        info  = stock.info
        if hist.empty:
            hist_summary = "No historical data."
        else:
            start = hist.index[0].strftime("%Y-%m-%d")
            end   = hist.index[-1].strftime("%Y-%m-%d")
            low   = hist["Low"].min()
            high  = hist["High"].max()
            avg_v = hist["Volume"].mean()
            one_year_ago = hist.index[-1] - pd.DateOffset(years=1)
            hist_1y = hist[hist.index >= one_year_ago]
            ret_1y  = ((hist_1y["Close"].iloc[-1] / hist_1y["Close"].iloc[0]) - 1) * 100 if len(hist_1y) > 1 else None
            ret_str = f", 1Y Return: {ret_1y:.1f}%" if ret_1y is not None else ""
            hist_summary = (f"5-year ({start} → {end}): Low ${low:.2f}, "
                            f"High ${high:.2f}, Avg Vol {avg_v:,.0f}{ret_str}")
        live = (f"Price: ${info.get('currentPrice','N/A')}, "
                f"Day Range: ${info.get('dayLow','N/A')}–${info.get('dayHigh','N/A')}, "
                f"Market Cap: ${info.get('marketCap',0):,}, "
                f"52W High: ${info.get('fiftyTwoWeekHigh','N/A')}, "
                f"52W Low: ${info.get('fiftyTwoWeekLow','N/A')}, "
                f"Sector: {info.get('sector','N/A')}")
        return hist_summary, live
    try:
        hist_s, live_s = cached(cache_key("yahoo", ticker), fetch)
        emit_progress("yahoo", "done")
        return {"yahoo_historical": hist_s, "yahoo_live": live_s}
    except Exception as e:
        emit_progress("yahoo", "error", str(e))
        return {"yahoo_historical": f"Error: {e}", "yahoo_live": "N/A",
                "errors": [f"Yahoo: {e}"]}

# ─────────────────────────────────────────────────────────────────────────
# KPI Calculator — 3 rich tables with formulas
# ─────────────────────────────────────────────────────────────────────────
def kpi_calculator_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("kpi", "running")
    ticker = state.get("ticker")
    if not ticker:
        return {"kpi_report": "No ticker."}
    def calc():
        stock = yf.Ticker(ticker)
        info  = stock.info
        hist  = stock.history(period="5y")

        # ── TABLE 1: Valuation Metrics ────────────────────────────────
        val_rows = []
        def v(label, raw, fmt, formula, interpretation):
            if isinstance(raw, (int, float)) and raw is not None:
                val = fmt.format(raw)
            else:
                val = "N/A"
            val_rows.append((label, val, formula, interpretation))

        v("P/E (Trailing)", info.get("trailingPE"),
          "{:.2f}x",
          "Market Price Per Share ÷ Trailing 12M EPS",
          "Valuation vs earnings of last 12 months")
        v("P/E (Forward)", info.get("forwardPE"),
          "{:.2f}x",
          "Market Price Per Share ÷ Next 12M EPS Estimate",
          "Forward-looking growth expectation priced in")
        v("Price / Book", info.get("priceToBook"),
          "{:.2f}x",
          "Market Price Per Share ÷ Book Value Per Share",
          "Premium paid over net asset value")
        v("Price / Sales", info.get("priceToSalesTrailing12Months"),
          "{:.2f}x",
          "Market Cap ÷ Trailing 12M Revenue",
          "Revenue-based valuation multiple")
        v("EV / EBITDA", info.get("enterpriseToEbitda"),
          "{:.2f}x",
          "Enterprise Value ÷ EBITDA",
          "Cash-flow adjusted total valuation")
        v("EV / Revenue", info.get("enterpriseToRevenue"),
          "{:.2f}x",
          "Enterprise Value ÷ Total Revenue",
          "Capital-structure-neutral revenue multiple")
        v("PEG Ratio", info.get("pegRatio"),
          "{:.2f}",
          "P/E ÷ EPS Growth Rate",
          "<1 = potentially undervalued relative to growth")

        table1 = (
            "\n### 📊 Table 1 — Valuation Metrics\n\n"
            "| Metric | Value | Formula | Interpretation |\n"
            "|--------|-------|---------|----------------|\n"
        )
        for row in val_rows:
            table1 += f"| {row[0]} | **{row[1]}** | `{row[2]}` | {row[3]} |\n"

        # ── TABLE 2: Profitability & Quality Metrics ───────────────────
        prof_rows = []
        def p(label, raw, fmt, formula, interpretation):
            if isinstance(raw, (int, float)) and raw is not None:
                val = fmt.format(raw)
            else:
                val = "N/A"
            prof_rows.append((label, val, formula, interpretation))

        p("Gross Margin", info.get("grossMargins"),
          "{:.2%}",
          "(Revenue − COGS) ÷ Revenue",
          "% of revenue retained after direct costs")
        p("Operating Margin", info.get("operatingMargins"),
          "{:.2%}",
          "Operating Income ÷ Revenue",
          "Core business profitability before interest/tax")
        p("Net Profit Margin", info.get("profitMargins"),
          "{:.2%}",
          "Net Income ÷ Revenue",
          "Bottom-line earnings efficiency")
        p("EBITDA Margin", info.get("ebitdaMargins"),
          "{:.2%}",
          "EBITDA ÷ Revenue",
          "Cash earnings power excluding non-cash items")
        p("Return on Equity (ROE)", info.get("returnOnEquity"),
          "{:.2%}",
          "Net Income ÷ Shareholders' Equity",
          "Earnings generated per dollar of equity capital")
        p("Return on Assets (ROA)", info.get("returnOnAssets"),
          "{:.2%}",
          "Net Income ÷ Total Assets",
          "How efficiently assets generate profit")
        p("Revenue Growth (YoY)", info.get("revenueGrowth"),
          "{:.2%}",
          "(Current Revenue − Prior Revenue) ÷ Prior Revenue",
          "Year-over-year top-line expansion rate")
        p("Earnings Growth (YoY)", info.get("earningsGrowth"),
          "{:.2%}",
          "(Current EPS − Prior EPS) ÷ |Prior EPS|",
          "Year-over-year bottom-line improvement")

        table2 = (
            "\n### 📊 Table 2 — Profitability & Growth Metrics\n\n"
            "| Metric | Value | Formula | Interpretation |\n"
            "|--------|-------|---------|----------------|\n"
        )
        for row in prof_rows:
            table2 += f"| {row[0]} | **{row[1]}** | `{row[2]}` | {row[3]} |\n"

        # ── TABLE 3: Risk, Technical & Capital Structure ───────────────
        risk_rows = []
        def r(label, val_str, formula, interpretation):
            risk_rows.append((label, val_str, formula, interpretation))

        beta = info.get("beta")
        r("Beta", f"{beta:.2f}" if isinstance(beta,(int,float)) else "N/A",
          "Cov(Stock,Market) ÷ Var(Market)",
          "Systematic market sensitivity; >1 = amplified moves")

        if not hist.empty:
            daily_ret = hist["Close"].pct_change().dropna()
            vol_ann   = daily_ret.std() * (252**0.5)
            r("Annualised Volatility (5Y)", f"{vol_ann:.2%}",
              "σ(daily returns) × √252",
              "Historical price risk over 5-year window")

            sharpe = None
            if daily_ret.std() > 0:
                sharpe = (daily_ret.mean()*252 - 0.045) / (daily_ret.std()*(252**0.5))
            r("Sharpe Ratio (5Y)", f"{sharpe:.2f}" if sharpe is not None else "N/A",
              "(Annualised Return − 4.5% RFR) ÷ Annualised Volatility",
              ">1 = strong risk-adjusted return")

            # Max drawdown
            roll_max = hist["Close"].cummax()
            drawdown = (hist["Close"] - roll_max) / roll_max
            max_dd   = drawdown.min()
            r("Max Drawdown (5Y)", f"{max_dd:.2%}",
              "(Trough Value − Peak Value) ÷ Peak Value",
              "Worst peak-to-trough decline over 5 years")

            for days in [50, 200]:
                ma  = hist["Close"].rolling(days).mean().iloc[-1]
                cur = hist["Close"].iloc[-1]
                pct = (cur / ma - 1) * 100 if pd.notna(ma) else None
                r(f"{days}-Day Moving Average", f"${ma:.2f}" if pd.notna(ma) else "N/A",
                  f"Average of last {days} closing prices",
                  f"Current price {'above' if pct and pct>0 else 'below'} MA by {abs(pct):.1f}%" if pct is not None else "Trend reference")

        debt_equity = info.get("debtToEquity")
        r("Debt / Equity", f"{debt_equity:.2f}" if isinstance(debt_equity,(int,float)) else "N/A",
          "Total Debt ÷ Shareholders' Equity",
          "Leverage level; higher = more financial risk")

        current_ratio = info.get("currentRatio")
        r("Current Ratio", f"{current_ratio:.2f}" if isinstance(current_ratio,(int,float)) else "N/A",
          "Current Assets ÷ Current Liabilities",
          ">2 = strong liquidity; <1 = potential short-term risk")

        quick_ratio = info.get("quickRatio")
        r("Quick Ratio", f"{quick_ratio:.2f}" if isinstance(quick_ratio,(int,float)) else "N/A",
          "(Current Assets − Inventory) ÷ Current Liabilities",
          "Stricter liquidity test excluding inventory")

        table3 = (
            "\n### 📊 Table 3 — Risk, Technical & Capital Structure Metrics\n\n"
            "| Metric | Value | Formula | Interpretation |\n"
            "|--------|-------|---------|----------------|\n"
        )
        for row in risk_rows:
            table3 += f"| {row[0]} | **{row[1]}** | `{row[2]}` | {row[3]} |\n"

        # ── Raw KPI string for downstream nodes ───────────────────────
        all_rows = val_rows + prof_rows + [(r[0], r[1], r[2], r[3]) for r in risk_rows]
        raw_kpi = "\n".join(f"{l}: {v}" for l,v,_,_ in all_rows)

        return (
            "## 7. Quantitative KPI Deep Dive\n\n"
            "The following three tables present all key performance indicators "
            "with the precise formula used to calculate each metric and an "
            "institutional-grade interpretation of what the value signals.\n"
            + table1 + table2 + table3 +
            "\n\n## KPI Raw Values (for downstream analysis)\n" + raw_kpi
        )

    try:
        report = cached(cache_key("kpi", ticker), calc)
        emit_progress("kpi", "done")
        return {"kpi_report": report}
    except Exception as e:
        emit_progress("kpi", "error", str(e))
        return {"kpi_report": f"KPI error: {e}", "errors": [f"KPI: {e}"]}

def kpi_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("kpi_analysis", "running")
    kpi = state.get("kpi_report","")
    if not kpi or "error" in kpi.lower():
        return {"kpi_analysis": "KPI data unavailable."}
    prompt = (f"Interpret these financial KPIs and give a structured assessment covering: "
              f"valuation (cheap/fair/expensive relative to sector norms), "
              f"profitability quality, technical trend signal, "
              f"and identify the single most important risk metric. "
              f"Use the KPI summary table and include the formula used to calculate each metric, "
              f"the numeric value, and what that value means for the company.\n\n{kpi}")
    resp = _llm().invoke([HumanMessage(content=prompt)])
    emit_progress("kpi_analysis", "done")
    return {"kpi_analysis": resp.content}

def risk_signal_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("risk_signal", "running")
    kpi      = state.get("kpi_report","")
    stocktw  = state.get("stocktwits_analysis","") or ""
    prompt   = (
        "Based on the following financial data, output EXACTLY one line:\n"
        "SIGNAL: Bull | Neutral | Bear\n"
        "REASON: <one sentence>\n\n"
        f"KPIs:\n{kpi[:1000]}\n\n"
        f"Social sentiment:\n{stocktw[:500]}"
    )
    try:
        resp = _llm().invoke([HumanMessage(content=prompt)]).content
        sig_m = re.search(r'SIGNAL:\s*(Bull|Neutral|Bear)', resp, re.IGNORECASE)
        rea_m = re.search(r'REASON:\s*(.+)', resp)
        signal = sig_m.group(1) if sig_m else "Neutral"
        reason = rea_m.group(1).strip() if rea_m else "Insufficient data."
        emit_progress("risk_signal", "done", signal)
        return {"risk_signal": signal, "risk_reason": reason}
    except Exception as e:
        return {"risk_signal": "Neutral", "risk_reason": f"Error: {e}"}

# ─────────────────────────────────────────────────────────────────────────
# NEW: 1-Year Profit/Loss Expectation Node
# ─────────────────────────────────────────────────────────────────────────
def profit_loss_expectation_node(state: AgentState) -> Dict[str, Any]:
    """
    Generates a structured 1-year forward profit/loss expectation
    using KPI data, risk signal, and available research.
    Produces a markdown section with bull/base/bear scenarios + probability table.
    """
    emit_progress("profit_loss_expectation", "running")
    ticker  = state.get("ticker", "STOCK")
    kpi     = state.get("kpi_report", "") or ""
    signal  = state.get("risk_signal", "Neutral")
    reason  = state.get("risk_reason", "")
    research = state.get("research_merged", "") or ""

    prompt = f"""You are a top-tier institutional equity analyst at Goldman Sachs.
Based on the financial data below, generate a comprehensive 1-YEAR PROFIT/LOSS EXPECTATION section for {ticker}.

REQUIRED OUTPUT FORMAT (markdown):

## 12. 1-Year Profit / Loss Expectation (Forward Outlook)

### Overview
[2 paragraphs: current positioning and what drives the 1-year outlook]

### Scenario Analysis

| Scenario | Probability | 1Y Price Target | Expected Return | Key Catalyst |
|----------|-------------|-----------------|-----------------|--------------|
| 🟢 Bull Case | XX% | $XXX | +XX% | [one catalyst] |
| 🟡 Base Case | XX% | $XXX | +XX% / -XX% | [one catalyst] |
| 🔴 Bear Case | XX% | $XXX | -XX% | [one catalyst] |

### Bull Case (Detailed)
[3 paragraphs: what needs to go right, what catalysts drive upside, price target methodology]

### Base Case (Detailed)
[3 paragraphs: most likely path, assumptions, realistic price target]

### Bear Case (Detailed)
[3 paragraphs: what could go wrong, downside risks, floor price estimate]

### Profit/Loss Summary Table

| Metric | Bull | Base | Bear |
|--------|------|------|------|
| Revenue Growth (1Y) | | | |
| EPS Growth (1Y) | | | |
| Expected P/E at EOY | | | |
| Price Target | | | |
| Expected Return | | | |
| Max Drawdown Risk | | | |

### Investment Horizon Recommendation
[1 paragraph: recommended holding period, entry points, exit triggers]

RULES:
- Use REAL numbers from the KPI data provided
- Overall signal is {signal} because: {reason}
- Probabilities must sum to 100%
- All price targets must be derived from valuation methodology (P/E expansion/contraction, DCF, etc.)
- Be specific — cite formula e.g. "Base P/E of 35x applied to forward EPS of $X = target of $Y"
- Do NOT use placeholder text

KPI DATA:
{kpi[:3000]}

RESEARCH CONTEXT:
{research[:2000]}
"""

    try:
        resp = _llm(max_tokens=2000).invoke([HumanMessage(content=prompt)])
        emit_progress("profit_loss_expectation", "done")
        return {"profit_loss_expectation": resp.content.strip()}
    except Exception as e:
        emit_progress("profit_loss_expectation", "error", str(e))
        return {"profit_loss_expectation": f"[1-Year expectation error: {e}]",
                "errors": [f"PnL expectation: {e}"]}

# ─────────────────────────────────────────────────────────────────────────
# Chart Generation — saves PNG files AND stores base64 in state
# ─────────────────────────────────────────────────────────────────────────
def _fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()

def generate_charts(ticker: str) -> List[dict]:
    stock = yf.Ticker(ticker)
    hist  = stock.history(period="5y")
    if hist.empty: return []
    os.makedirs("charts", exist_ok=True)
    charts = []

    def save_chart(title, plot_func):
        fig, ax = plt.subplots(figsize=(11, 5))
        plot_func(ax)
        ax.set_title(f"{ticker} — {title}", fontsize=13, fontweight="bold")
        ax.grid(alpha=0.25)
        fname = f"charts/{ticker}_{title.replace(' ','_').replace('/','_')}.png"
        plt.tight_layout()
        plt.savefig(fname, dpi=110, bbox_inches="tight")
        b64 = _fig_to_base64(fig)
        plt.close()
        return fname, b64

    # 1. Price + 50/200 MA
    def price_ma(ax):
        ax.plot(hist.index, hist["Close"], color="#1f6feb", lw=1.4, label="Close")
        ax.plot(hist.index, hist["Close"].rolling(50).mean(),  color="orange", lw=1, ls="--", label="MA50")
        ax.plot(hist.index, hist["Close"].rolling(200).mean(), color="red",    lw=1, ls="--", label="MA200")
        ax.legend(fontsize=8); ax.set_ylabel("Price ($)")
    f, b64 = save_chart("Price + MA (5Y)", price_ma)
    charts.append({"type":"Price + MA (5Y)","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    # 2. RSI 14
    delta = hist["Close"].diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    rsi   = 100 - (100 / (1 + gain.rolling(14).mean() / loss.rolling(14).mean()))
    def rsi_plot(ax):
        ax.plot(hist.index, rsi, color="purple", lw=1.2)
        ax.axhline(70, ls="--", color="red",   alpha=0.5, lw=0.8)
        ax.axhline(30, ls="--", color="green", alpha=0.5, lw=0.8)
        ax.fill_between(hist.index, rsi, 70, where=(rsi>=70), alpha=0.15, color="red")
        ax.fill_between(hist.index, rsi, 30, where=(rsi<=30), alpha=0.15, color="green")
        ax.set_ylim(0, 100); ax.set_ylabel("RSI")
    f, b64 = save_chart("RSI (14)", rsi_plot)
    charts.append({"type":"RSI (14)","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    # 3. MACD
    exp12  = hist["Close"].ewm(span=12, adjust=False).mean()
    exp26  = hist["Close"].ewm(span=26, adjust=False).mean()
    macd   = exp12 - exp26
    signal_line = macd.ewm(span=9, adjust=False).mean()
    hist_macd   = macd - signal_line
    def macd_plot(ax):
        ax.plot(hist.index, macd,        label="MACD",   color="black",  lw=1.2)
        ax.plot(hist.index, signal_line, label="Signal", color="orange", lw=1)
        ax.bar(hist.index, hist_macd,
               color=["#26a641" if v >= 0 else "#e74c3c" for v in hist_macd],
               alpha=0.4, width=1)
        ax.legend(fontsize=8)
    f, b64 = save_chart("MACD", macd_plot)
    charts.append({"type":"MACD","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    # 4. Volume
    def vol_plot(ax):
        colors = ["#26a641" if c >= o else "#e74c3c"
                  for c, o in zip(hist["Close"], hist["Open"])]
        ax.bar(hist.index, hist["Volume"], color=colors, alpha=0.7, width=1)
        ax.set_ylabel("Volume")
    f, b64 = save_chart("Volume", vol_plot)
    charts.append({"type":"Volume","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    # 5. Bollinger Bands
    sma     = hist["Close"].rolling(20).mean()
    std     = hist["Close"].rolling(20).std()
    upper   = sma + 2*std
    lower_b = sma - 2*std
    def bb_plot(ax):
        ax.plot(hist.index, hist["Close"], color="#1f6feb", lw=1.2, label="Close")
        ax.plot(hist.index, sma,     color="orange", lw=1,   ls="--", label="SMA20")
        ax.plot(hist.index, upper,   color="gray",   lw=0.8, ls="--", label="Upper")
        ax.plot(hist.index, lower_b, color="gray",   lw=0.8, ls="--", label="Lower")
        ax.fill_between(hist.index, upper, lower_b, alpha=0.08, color="blue")
        ax.legend(fontsize=8)
    f, b64 = save_chart("Bollinger Bands", bb_plot)
    charts.append({"type":"Bollinger Bands","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    # 6. 6-Month price
    cutoff = hist.index[-1] - pd.DateOffset(months=6)
    six    = hist.loc[hist.index >= cutoff]
    if not six.empty:
        def sixm_plot(ax):
            ax.plot(six.index, six["Close"], color="darkblue", lw=1.5)
            ax.set_ylabel("Price ($)")
        f, b64 = save_chart("Price (6M)", sixm_plot)
        charts.append({"type":"Price (6M)","file":f,"image_base64":b64,"pre_analysis":"","post_analysis":""})

    return charts

def chart_analysis_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("chart_gen", "running")
    ticker = state.get("ticker")
    if not ticker: return {"charts": []}
    try:
        charts = cached(cache_key("charts", ticker), lambda: generate_charts(ticker))
        llm    = _llm()

        chart_context = {
            "Price + MA (5Y)":  "This is a 5-year price chart with 50-day and 200-day moving averages. A 'golden cross' (MA50 crossing above MA200) is bullish; a 'death cross' is bearish. Check if price is above/below both MAs.",
            "RSI (14)":         "RSI measures momentum. Above 70 = overbought (potential sell signal). Below 30 = oversold (potential buy signal). Divergences between RSI and price are especially significant.",
            "MACD":             "MACD shows momentum shifts. Bullish signal = MACD crosses above signal line. Green histogram bars = positive momentum. Watch for convergence/divergence with price action.",
            "Volume":           "Volume confirms price moves. Rising price + rising volume = strong trend. Rising price + falling volume = weak trend. Volume spikes often mark reversals.",
            "Bollinger Bands":  "Price touching upper band = overbought; lower band = oversold. Band squeeze (narrowing) = volatility compression before a big move. Watch for breakouts from the bands.",
            "Price (6M)":       "Recent 6-month price action shows current trend. Look for support/resistance levels, recent highs/lows, and momentum direction heading into the near term.",
        }

        for ch in charts:
            chart_type = ch["type"]
            context = chart_context.get(chart_type, f"This is a {chart_type} technical chart.")
            ch["pre_analysis"] = context

            msg = HumanMessage(content=[
                {"type": "text", "text": (
                    f"You are a professional technical analyst. Carefully examine this "
                    f"{chart_type} chart for {ticker}.\n\n"
                    f"Provide a precise technical analysis covering:\n"
                    f"1. **Current Trend**: Direction (uptrend/downtrend/sideways) and strength\n"
                    f"2. **Key Levels**: Important support/resistance prices you can read from the chart\n"
                    f"3. **Signals**: Any bullish/bearish signals visible (crossovers, divergences, extremes)\n"
                    f"4. **Momentum**: Acceleration or deceleration in recent weeks\n"
                    f"5. **Actionable Verdict**: One clear statement — is this chart bullish, bearish, or neutral?\n\n"
                    f"Be specific with price levels and dates where visible. Professional, concise language."
                )},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{ch['image_base64']}"}}
            ])
            try:
                ch["post_analysis"] = llm.invoke([msg]).content
            except Exception as ve:
                ch["post_analysis"] = f"Vision analysis error: {ve}"

        emit_progress("chart_gen", "done", f"{len(charts)} charts")
        return {"charts": charts}
    except Exception as e:
        emit_progress("chart_gen", "error", str(e))
        return {"charts":[], "errors":[f"Charts: {e}"]}

def analyst_reducer_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("analyst_reducer", "running")
    parts = ["# QUANTITATIVE ANALYSIS"]
    if state.get("yahoo_historical"): parts.append(f"**Historical:** {state['yahoo_historical']}")
    if state.get("yahoo_live"):       parts.append(f"**Live:**       {state['yahoo_live']}")
    if state.get("kpi_report"):       parts.append(f"**KPIs:**\n{state['kpi_report']}")
    if state.get("kpi_analysis"):     parts.append(f"**KPI Interpretation:**\n{state['kpi_analysis']}")
    if state.get("risk_signal"):
        parts.append(f"**Risk Signal:** {state['risk_signal']} — {state.get('risk_reason','')}")
    if state.get("profit_loss_expectation"):
        parts.append(f"**1-Year P&L Expectation:**\n{state['profit_loss_expectation']}")

    charts = state.get("charts", [])
    if charts:
        parts.append("**Technical Chart Analyses:**")
        for ch in charts:
            parts.append(
                f"### {ch['type']}\n"
                f"**Context:** {ch.get('pre_analysis','')}\n"
                f"**GPT Vision Analysis:** {ch.get('post_analysis','')}\n"
                f"**File:** {ch.get('file','')}\n"
            )
    emit_progress("analyst_reducer", "done")
    return {"analyst_merged": "\n\n".join(parts)}

# ─────────────────────────────────────────────────────────────────────────
# Writer Node — Fan-out task structure, 20+ page target
# ─────────────────────────────────────────────────────────────────────────
WRITER_SYSTEM = """You are a Managing Director-level equity analyst at Goldman Sachs.
You write the most comprehensive, detailed stock research reports on Wall Street.
TARGET LENGTH: 7000-9000 words minimum (equivalent to at least 20 printed pages).

TASK-BASED WRITING APPROACH:
You will write each section as a separate analytical task, then integrate all tasks
into one cohesive, seamlessly flowing professional report.

FORMATTING RULES:
- Title: # {ticker} Equity Research Report
- Use ## for main sections, ### for sub-sections
- Use markdown tables for all comparisons, risk matrices, scenarios, KPIs
- Bold all key numbers, signals, and verdicts
- Dense paragraphs: minimum 5-6 sentences each with real data
- NEVER use placeholder text — analyze what you have
- Cite sources inline using exact citation_text strings
- Minimum 5 distinct source citations
- Use clean Markdown numbering only: "1.", "2.", "3."; never use "1)" or cramped inline numbering
- Every numbered item must start on its own new line with a blank line before the list when needed
- Keep the report professional, readable, and client-ready with clear headings, tables, and spacing

CHART PLACEHOLDER RULES (CRITICAL):
- Write EXACTLY [CHART GOES HERE] for each of the 6 technical charts
- 2 sentences context BEFORE each placeholder
- 3-4 sentences analysis AFTER each placeholder
- All 6 must appear — do not skip any

IMAGE PLACEHOLDER RULES:
- Do NOT add [[IMAGE_N]] placeholders yourself
- The image_planner node will insert each image beside the section it explains

REPORT STRUCTURE:
Each section below is a TASK. Prepare it as a focused analytical task, then join all tasks.
"""

def writer_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("writer", "running")
    ticker   = state.get("ticker") or state.get("query","STOCK")
    research = state.get("research_merged") or "No research data available."
    analyst  = state.get("analyst_merged")  or "No analyst data available."
    kpi      = state.get("kpi_report")      or "No KPI data."
    signal   = state.get("risk_signal")     or "Neutral"
    reason   = state.get("risk_reason")     or ""
    pnl_exp  = state.get("profit_loss_expectation") or "Not available."

    citation_ref = ""
    for cit in state.get("tavily_citations", []):
        citation_ref += f"  {cit['citation_text']} → {cit['title']} ({cit['url']})\n"

    charts_text = ""
    for ch in state.get("charts", []):
        charts_text += (
            f"\n### Chart: {ch['type']}\n"
            f"**What to look for:** {ch.get('pre_analysis','')}\n"
            f"**Technical Reading:** {(ch.get('post_analysis',''))[:500]}\n"
        )

    research_trim = research[:5000]
    analyst_trim  = analyst[:4500]
    kpi_trim      = kpi[:3000]
    pnl_trim      = pnl_exp[:2000]

    prompt = f"""Write a COMPLETE, EXHAUSTIVE 20+ PAGE institutional equity research report for **{ticker}**.
Overall Risk Signal: **{signal}** — {reason}
TARGET: 7000-9000 words minimum. Dense, data-rich, institutional quality.
The customer must receive a polished client-ready report with clean numbering, no cramped "1)" style lists, and proper line breaks.

Available citations — use these EXACT citation_text strings inline:
{citation_ref}

════════════════════════════════════════════════════════
FAN-OUT TASK STRUCTURE
Prepare each numbered section below as a SEPARATE ANALYTICAL TASK.
After all tasks are prepared, INTEGRATE them into one seamless report of at least 20 pages.
════════════════════════════════════════════════════════

# {ticker} Equity Research Report
**Date:** {datetime.now().strftime("%B %d, %Y")} | **Rating:** [BUY/HOLD/SELL] | **Risk Signal:** {signal}

---

## TASK 1 — Executive Summary
[Write 4 rich paragraphs]:
- Para 1: Company snapshot — what it does, why it matters, current market position
- Para 2: Investment thesis — the single most important reason to own or avoid
- Para 3: Key financial metrics summary — revenue, margins, valuation with real numbers
- Para 4: Recommendation and 12-month outlook with price target range

[Do not place generated image placeholders here; the image planner will add each visual in its relevant section.]

---

## TASK 2 — Company Overview & Business Model
[Write 5 paragraphs]:
- Para 1: Core business segments and revenue breakdown
- Para 2: Competitive moat and sustainable advantage analysis
- Para 3: Total addressable market (TAM) and penetration
- Para 4: Key customers, partnerships, and revenue concentration risk
- Para 5: Management team quality and execution track record

---

## TASK 3 — Financial Fundamentals Deep Dive

### 3.1 Revenue & Growth Analysis
[Task: 3 paragraphs on revenue trajectory, growth drivers, segment analysis]

### 3.2 Profitability Analysis
[Task: 3 paragraphs on margins — gross, operating, net — trends and sustainability]

### 3.3 Balance Sheet & Cash Flow
[Task: 2 paragraphs on debt, cash, FCF generation and capital allocation policy]

### 3.4 Valuation Deep Dive
[Task: 2 paragraphs — current multiples vs history, sector peers, DCF range]

---

## TASK 4 — Market & Analyst Sentiment Analysis
[Task: 3 paragraphs — Wall Street consensus, target price range, recent upgrades/downgrades, retail sentiment from StockTwits]

---

## TASK 5 — SEC Regulatory & Filing Analysis
[Task: 2 paragraphs — recent 10-K/10-Q/8-K filings, any disclosed risks, compliance posture]

---

## TASK 6 — Technical Analysis — All 6 Charts
CRITICAL: Write [CHART GOES HERE] placeholder EXACTLY as shown for each chart.

### 1. Price + Moving Averages (5-Year)
[Write 2 context sentences — what MA crossovers mean for {ticker}]
[CHART GOES HERE]
[Write 4 sentences of specific technical analysis based on chart data]

### 2. RSI Momentum (14-Day)
[Write 2 context sentences — momentum interpretation for {ticker}]
[CHART GOES HERE]
[Write 4 sentences of RSI analysis — overbought/oversold, divergences]

### 3. MACD Signal
[Write 2 context sentences — MACD interpretation for {ticker}]
[CHART GOES HERE]
[Write 4 sentences — crossover signals, histogram trend, momentum]

### 4. Volume Analysis
[Write 2 context sentences — volume confirms price action]
[CHART GOES HERE]
[Write 4 sentences — volume trends, accumulation/distribution, conviction]

### 5. Bollinger Bands
[Write 2 context sentences — volatility and mean-reversion for {ticker}]
[CHART GOES HERE]
[Write 4 sentences — band width, price position, squeeze signals]

### 6. Recent Price Action (6-Month)
[Write 2 context sentences — near-term momentum]
[CHART GOES HERE]
[Write 4 sentences — key support/resistance, recent momentum, trend direction]

### 6.7 Technical Summary Verdict
[Task: 1 paragraph synthesizing all 6 chart signals into a single technical verdict]

---

## TASK 7 — Quantitative KPI Deep Dive

{kpi_trim}

[Task: After the tables above, write 3 paragraphs interpreting the most critical KPIs:
1. Valuation interpretation — is the stock cheap, fair, or expensive vs peers?
2. Profitability and quality assessment — are margins sustainable?
3. Risk metrics — what does volatility, beta, and leverage tell us?]

---

## TASK 8 — Risk Factor Matrix

[Task: Build a comprehensive 8-row risk matrix]

| # | Risk Factor | Description | Probability | Severity | Mitigation Strategy |
|---|-------------|-------------|-------------|----------|---------------------|
| 1 | | | HIGH/MED/LOW | HIGH/MED/LOW | |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |
| 5 | | | | | |
| 6 | | | | | |
| 7 | | | | | |
| 8 | | | | | |

[Task: Write 2 paragraphs explaining the top 3 risks in depth]

---

## TASK 9 — Bull Case vs Bear Case vs Base Case

### 9.1 Bull Case — [+XX% upside target]
[Task: 3 paragraphs — what needs to go right, catalysts, price target methodology]

### 9.2 Bear Case — [-XX% downside target]
[Task: 3 paragraphs — what could go wrong, downside risks, floor price estimate]

### 9.3 Base Case — Most Likely Path
[Task: 3 paragraphs — consensus path, key assumptions, 12-month target]

### Scenario Comparison Table
| Scenario | Probability | 12M Target | Expected Return | Key Catalyst | Key Risk |
|----------|-------------|-----------|-----------------|--------------|----------|
| Bull | | | | | |
| Base | | | | | |
| Bear | | | | | |

---

## TASK 10 — Valuation & Final Recommendation

### 10.1 Price Target Methodology
[Task: 2 paragraphs — explain DCF assumptions, comparable multiples methodology, blended target]

### 10.2 Final Verdict
[Task: 3 paragraphs — clear BUY/HOLD/SELL with price target, conviction level, and why this rating is appropriate now]

**FINAL RATING: [BUY / HOLD / SELL]**
**12-Month Price Target: $XXX**
**Current Price: $XXX**
**Expected Return: XX%**
**Risk Signal: {signal}**

### 10.3 One-Year Prediction Summary
Create a clean client-ready table with these columns:

| Horizon | Base Case Price | Bull Case Price | Bear Case Price | Most Likely Rating | Expected Return | Key Driver |
|---------|-----------------|-----------------|-----------------|--------------------|-----------------|------------|
| 3 Months | | | | BUY/HOLD/SELL | | |
| 6 Months | | | | BUY/HOLD/SELL | | |
| 12 Months | | | | BUY/HOLD/SELL | | |

Then write a final 2-paragraph explanation of the one-year prediction, including catalysts, risks, and what would make the rating change.

---

## TASK 11 — 1-Year Profit / Loss Expectation

{pnl_trim}

---

## TASK 12 — Appendix — Sources & Data
[Task: List all Tavily citations and data sources used]

════════════════════════════════════════════════════════
INTEGRATION INSTRUCTIONS:
After preparing all 12 tasks, join them into one seamless, flowing 20+ page report.
Ensure logical transitions between sections.
Maintain consistent tone — institutional, data-driven, professional.
Total word count target: 7000-9000 words.

After the report, on separate lines, write EXACTLY:
CONFIDENCE: <0-100>
SECTION_SCORES: executive=<0-100>,fundamentals=<0-100>,sentiment=<0-100>,quant=<0-100>,risks=<0-100>
════════════════════════════════════════════════════════

Research Data:
{research_trim}

Quantitative & Technical Data:
{analyst_trim}
"""

    try:
        resp = _llm(max_tokens=8192).invoke([
            SystemMessage(content=WRITER_SYSTEM),
            HumanMessage(content=prompt)
        ])
        text = resp.content

        conf_m     = re.search(r'CONFIDENCE:\s*(\d+)', text)
        confidence = float(conf_m.group(1))/100.0 if conf_m else 0.5

        sec_m = re.search(r'SECTION_SCORES:\s*(.+)', text)
        section_scores = {}
        if sec_m:
            for part in sec_m.group(1).split(","):
                kv = part.strip().split("=")
                if len(kv) == 2:
                    try: section_scores[kv[0].strip()] = int(kv[1].strip())
                    except: pass

        draft = re.sub(r'CONFIDENCE:\s*\d+', '', text)
        draft = re.sub(r'SECTION_SCORES:\s*.+', '', draft).strip()

        emit_progress("writer", "done", f"conf={confidence:.0%}")
        return {
            "draft_report_markdown": draft,
            "confidence_score":      confidence,
            "section_scores":        section_scores,
        }
    except Exception as e:
        emit_progress("writer", "error", str(e))
        return {
            "draft_report_markdown": f"Report failed: {e}",
            "confidence_score":      0.0,
            "section_scores":        {},
        }

# ─────────────────────────────────────────────────────────────────────────
# Image Planner Node — targets 5 images
# ─────────────────────────────────────────────────────────────────────────
IMAGE_PLANNER_SYSTEM = """You are an expert technical editor and data visualisation specialist.

Your job: Read a stock research report and decide WHERE to insert EXACTLY 5 additional images/diagrams
that would materially improve reader understanding.

CRITICAL RULE — DO NOT suggest any of these chart types, they already exist as embedded charts:
- Price charts (moving averages, price history, 5Y price, 6M price)
- RSI charts
- MACD charts
- Volume charts
- Bollinger Band charts

MANDATORY: You MUST produce EXACTLY 5 images. No fewer, no more.

CHOOSE 5 from these categories (pick the most relevant for this stock):
1. Business model / revenue stream diagram
2. Competitive landscape comparison bar chart (company vs top 3-4 peers on key metrics)
3. Risk matrix heatmap (probability vs severity grid)
4. Valuation multiples comparison bar chart (P/E, EV/EBITDA vs sector peers)
5. Revenue/earnings growth timeline (last 4 years + 2 year forward estimate)
6. Geographic revenue breakdown pie chart
7. Bull vs Bear scenario probability distribution
8. Market share visualization (pie or bar)
9. Product portfolio / segment revenue breakdown
10. EPS growth trajectory with analyst estimates

PLACEMENT RULES:
- Distribute the 5 image placeholders across the report; do not cluster them in one section
- Place each placeholder immediately after the paragraph or table that its visual explains
- Use at most one generated-image placeholder per main section
- Good target sections are Company Overview/Business Model, Financial Fundamentals, Competitive Landscape, Valuation/Scenarios, and Risks
- Do NOT insert generated-image placeholders in the Executive Summary or before the first section
- Before each placeholder, write 1-2 sentences explaining why the image is needed and what it illustrates
- Insert placeholders EXACTLY as: [[IMAGE_1]], [[IMAGE_2]], [[IMAGE_3]], [[IMAGE_4]], [[IMAGE_5]]

For each image, write a DETAILED Gemini prompt (4-5 sentences):
- Specify: exact chart type, data to show, colors (professional blue/gray/white palette)
- Style: "clean professional financial infographic, white background, clearly labeled, institutional quality"
- Be specific about labels, numbers, axes, legends
- Mention specific company name and the data to visualize

Return valid GlobalImagePlan JSON with md_with_placeholders and exactly 5 images.
"""

def _image_target_terms(spec: dict, index: int) -> List[str]:
    text = " ".join(str(spec.get(k, "")) for k in ("filename", "alt", "caption", "prompt")).lower()
    if any(k in text for k in ("risk", "heatmap", "probability", "severity")):
        return ["risk", "risks"]
    if any(k in text for k in ("valuation", "multiple", "scenario", "bull", "bear")):
        return ["valuation", "scenario", "investment thesis"]
    if any(k in text for k in ("competitive", "peer", "market share", "landscape")):
        return ["competitive", "competition", "market"]
    if any(k in text for k in ("financial", "revenue", "earnings", "growth", "eps", "margin")):
        return ["financial", "fundamental", "revenue", "earnings"]
    if any(k in text for k in ("business model", "segment", "product", "geographic", "portfolio")):
        return ["company overview", "business model", "segment", "product"]

    fallback = [
        ["company overview", "business model"],
        ["financial", "fundamental"],
        ["competitive", "market"],
        ["valuation", "scenario"],
        ["risk"],
    ]
    return fallback[min(index, len(fallback) - 1)]

def _find_section_insert_line(lines: List[str], terms: List[str], used_lines: set[int]) -> int:
    for i, line in enumerate(lines):
        clean = line.strip().lower()
        if clean.startswith("## ") and any(term in clean for term in terms):
            while i + 1 < len(lines) and not lines[i + 1].strip():
                i += 1
            insert_line = i + 1
            if insert_line not in used_lines:
                return insert_line

    headings = [i + 1 for i, line in enumerate(lines) if line.strip().startswith("## ")]
    headings = [line for line in headings if line not in used_lines]
    if headings:
        return headings[min(len(used_lines), len(headings) - 1)]
    return len(lines)

def _placeholders_are_clustered(md: str, placeholders: List[str]) -> bool:
    positions = []
    for idx, line in enumerate(md.splitlines()):
        if any(ph in line for ph in placeholders):
            positions.append(idx)
    if len(positions) < 2:
        return False
    return max(positions) - min(positions) <= 25

def _redistribute_image_placeholders(md: str, specs: List[dict]) -> str:
    placeholders = [spec.get("placeholder", "") for spec in specs if spec.get("placeholder")]
    if not placeholders or not _placeholders_are_clustered(md, placeholders):
        return md

    lines = [
        line for line in md.splitlines()
        if not any(ph in line for ph in placeholders)
    ]
    used_lines: set[int] = set()
    inserts: List[tuple[int, str]] = []

    for idx, spec in enumerate(specs):
        placeholder = spec.get("placeholder")
        if not placeholder:
            continue
        terms = _image_target_terms(spec, idx)
        line_no = _find_section_insert_line(lines, terms, used_lines)
        used_lines.add(line_no)
        caption = spec.get("caption") or spec.get("alt") or "This visual supports the analysis in this section."
        inserts.append((line_no, f"\n{caption}\n{placeholder}\n"))

    for line_no, block in sorted(inserts, reverse=True):
        lines.insert(line_no, block)

    return "\n".join(lines)

def image_planner_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("image_planner", "running")
    draft = state.get("draft_report_markdown","")
    if not draft:
        return {"md_with_placeholders": "", "image_specs": []}

    try:
        planner = _llm().with_structured_output(GlobalImagePlan)
        ticker  = state.get("ticker","STOCK")

        image_plan = planner.invoke([
            SystemMessage(content=IMAGE_PLANNER_SYSTEM),
            HumanMessage(content=(
                f"Ticker: {ticker}\n"
                f"You MUST insert exactly 5 images in the relevant sections, spread across the report.\n"
                f"Do not place them all after the Executive Summary.\n"
                f"Report follows. Decide image placement and write Gemini prompts.\n\n"
                f"{draft[:8000]}"
            ))
        ])

        image_specs = [img.model_dump() for img in image_plan.images]
        md_with_placeholders = _redistribute_image_placeholders(
            image_plan.md_with_placeholders,
            image_specs,
        )

        emit_progress("image_planner", "done", f"{len(image_plan.images)} images planned")
        return {
            "md_with_placeholders": md_with_placeholders,
            "image_specs": image_specs,
        }
    except Exception as e:
        emit_progress("image_planner", "error", str(e))
        logger.error(f"Image planner: {e}")
        return {
            "md_with_placeholders": draft,
            "image_specs": [],
        }

# ─────────────────────────────────────────────────────────────────────────
# Gemini Image Generation
# ─────────────────────────────────────────────────────────────────────────
_GEMINI_IMAGE_MODELS = [
    "gemini-2.5-flash-image",
]

def _extract_image_from_response(resp) -> Optional[bytes]:
    parts = getattr(resp, "parts", None)
    if not parts and getattr(resp, "candidates", None):
        try:    parts = resp.candidates[0].content.parts
        except: parts = None
    if not parts:
        return None
    for part in parts:
        inline = getattr(part, "inline_data", None)
        if inline and getattr(inline, "data", None):
            raw = inline.data
            if isinstance(raw, (bytes, bytearray)):
                return bytes(raw)
            else:
                return base64.b64decode(raw)
    return None

def _gemini_generate_image_bytes(prompt: str) -> bytes:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is not set.")

    try:
        from google import genai
        from google.genai import types
    except ImportError:
        raise RuntimeError(
            "google-genai not installed. Run:\n"
            "  pip install google-genai Pillow"
        )

    client = genai.Client(api_key=api_key)
    last_err = None

    for model_name in _GEMINI_IMAGE_MODELS:
        try:
            resp = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_modalities=["IMAGE", "TEXT"],
                ),
            )
            img_bytes = _extract_image_from_response(resp)
            if img_bytes:
                logger.success(f"Gemini image generated via {model_name}")
                return img_bytes
            else:
                logger.warning(f"{model_name}: response had no image data, trying next...")
                last_err = RuntimeError(f"{model_name} returned no image data")
        except Exception as e:
            logger.warning(f"{model_name} failed: {e}")
            last_err = e

    raise RuntimeError(
        f"All Gemini image models failed. Last error: {last_err}\n"
        f"Models tried: {_GEMINI_IMAGE_MODELS}\n"
        f"Check your GOOGLE_API_KEY and billing."
    )

def _ticker_image_filename(ticker: str, filename: str, prompt: str) -> str:
    raw_name = Path(filename or "generated_image.png").name
    stem = re.sub(r"[^a-zA-Z0-9_-]+", "_", Path(raw_name).stem).strip("_") or "generated_image"
    suffix = Path(raw_name).suffix.lower() or ".png"
    ticker_clean = re.sub(r"[^a-zA-Z0-9]+", "_", ticker or "STOCK").strip("_").upper() or "STOCK"
    random_id = uuid.uuid4().hex[:10]
    return f"{ticker_clean}_{stem}_{random_id}{suffix}"

# ─────────────────────────────────────────────────────────────────────────
# gemini_image_node — assembles final markdown with ALL images embedded
# ─────────────────────────────────────────────────────────────────────────
def gemini_image_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("gemini_image", "running")
    plan   = state.get("image_specs", []) or []
    ticker = state.get("ticker", "STOCK")
    charts = state.get("charts", [])

    md = state.get("md_with_placeholders") or state.get("draft_report_markdown", "")
    if not md:
        return {"final_report_markdown": "No report content."}

    images_dir = Path("images")
    images_dir.mkdir(exist_ok=True)

    # ── Step 1: Generate Gemini images → embed as base64 ─────────────────
    for spec in plan:
        placeholder = spec["placeholder"]
        prompt      = (
            f"Create this visual specifically for {ticker}. "
            f"Every company label, title, and context must refer to {ticker}, not NVIDIA/NVDA unless {ticker} is NVDA. "
            f"{spec.get('prompt', '')}"
        )
        filename    = _ticker_image_filename(ticker, spec.get("filename", ""), prompt)
        out_path    = images_dir / filename

        if not out_path.exists():
            try:
                img_bytes = _gemini_generate_image_bytes(prompt)
                out_path.write_bytes(img_bytes)
                logger.success(f"Generated image: {filename}")
            except Exception as e:
                logger.error(f"Gemini image failed for {filename}: {e}")
                fallback = (
                    f"\n> **[IMAGE GENERATION FAILED]** {spec.get('caption','')}\n"
                    f"> Error: {e}\n"
                )
                md = md.replace(placeholder, fallback)
                continue
        else:
            logger.info(f"Reusing cached image: {filename}")

        try:
            img_b64 = base64.b64encode(out_path.read_bytes()).decode()
            img_md  = (
                f"\n![{spec['alt']}](data:image/png;base64,{img_b64})\n"
                f"*{spec['caption']}*\n"
            )
        except Exception as e:
            logger.error(f"Failed to read image {filename}: {e}")
            img_md = f"\n> **[Could not embed image: {filename}]** Error: {e}\n"

        md = md.replace(placeholder, img_md)

    # ── Step 2: Replace [CHART GOES HERE] with base64-embedded charts ────
    chart_fallbacks = []
    for ch in charts:
        b64  = ch.get("image_base64", "")
        name = ch.get("type", "Chart")
        post = ch.get("post_analysis", "")

        if b64:
            chart_block = (
                f"\n![{name}](data:image/png;base64,{b64})\n"
                f"\n**Technical Analysis:** {post}\n"
            )
        else:
            fpath = ch.get("file", "")
            if fpath and Path(fpath).exists():
                file_b64 = base64.b64encode(Path(fpath).read_bytes()).decode()
                chart_block = (
                    f"\n![{name}](data:image/png;base64,{file_b64})\n"
                    f"\n**Technical Analysis:** {post}\n"
                )
            else:
                chart_block = f"\n> **[Chart: {name} — file not found]**\n\n**Technical Analysis:** {post}\n"

        if "[CHART GOES HERE]" in md:
            md = md.replace("[CHART GOES HERE]", chart_block, 1)
        else:
            chart_fallbacks.append(chart_block)

    if chart_fallbacks:
        md += "\n\n---\n## Technical Charts\n"
        md += "\n".join(chart_fallbacks)

    # ── Step 3: Make ALL Tavily citations clickable hyperlinks ────────────
    citation_map = {}
    for cit in state.get("tavily_citations", []):
        raw_text = cit["citation_text"]
        url      = cit["url"]
        title    = cit.get("title", "Source")
        link     = f"[{raw_text}]({url} \"{title}\")"
        citation_map[raw_text] = link

    for raw_text, link in citation_map.items():
        pattern = r'(?<!\[)' + re.escape(raw_text) + r'(?!\])'
        md = re.sub(pattern, link, md)

    # ── Step 4: Save final markdown ───────────────────────────────────────
    os.makedirs("reports", exist_ok=True)
    md_path = f"reports/{ticker}_final_report.md"
    Path(md_path).write_text(md, encoding="utf-8")
    logger.success(f"Final report saved: {md_path}")

    # ── Step 5: Optional PDF export ───────────────────────────────────────
    pdf_path = None
    if PDF_AVAILABLE:
        pdf_path = export_pdf(md, ticker)

    emit_progress("gemini_image", "done", f"images={len(plan)}, charts={len(charts)}")
    return {
        "final_report_markdown": md,
        "final_report_pdf":      pdf_path,
    }

# ─────────────────────────────────────────────────────────────────────────
# PDF Exporter
# ─────────────────────────────────────────────────────────────────────────
def export_pdf(report_md: str, ticker: str) -> Optional[str]:
    if not PDF_AVAILABLE:
        logger.warning("reportlab not installed — skipping PDF export")
        return None
    path = f"reports/{ticker}_report.pdf"
    os.makedirs("reports", exist_ok=True)
    doc    = SimpleDocTemplate(path, pagesize=A4,
                               rightMargin=2*cm, leftMargin=2*cm,
                               topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    story  = []
    for line in report_md.split("\n"):
        if "data:image/png;base64," in line:
            story.append(Paragraph("[Chart Image — see markdown version]", styles["Italic"]))
            continue
        clean = re.sub(r'\*\*|!\[.*?\]\(.*?\)', '', line).strip()
        if not clean:
            story.append(Spacer(1, 0.2*cm)); continue
        if clean.startswith("###"):
            story.append(Paragraph(clean.lstrip("#").strip(), styles["Heading3"]))
        elif clean.startswith("##"):
            story.append(Paragraph(clean.lstrip("#").strip(), styles["Heading2"]))
        elif clean.startswith("#"):
            story.append(Paragraph(clean.lstrip("#").strip(), styles["Heading1"]))
        else:
            story.append(Paragraph(clean, styles["BodyText"]))
    doc.build(story)
    logger.success(f"PDF saved: {path}")
    return path

# ─────────────────────────────────────────────────────────────────────────
# Build Graph
# ─────────────────────────────────────────────────────────────────────────

def analyst_gate_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("analyst_gate", "running")
    return {}

def writer_gate_node(state: AgentState) -> Dict[str, Any]:
    emit_progress("writer_gate", "running")
    return {}

builder = StateGraph(AgentState)

# ── register nodes ──────────────────────────────────────────────────────
builder.add_node("orchestrator",              orchestrator_supervisor)
builder.add_node("research_supervisor",       research_supervisor_node)
builder.add_node("tavily",                    tavily_node)
builder.add_node("reuters_marketwatch",       reuters_marketwatch_node)
builder.add_node("stocktwits",                stocktwits_node)
builder.add_node("edgar",                     sec_edgar_node)
builder.add_node("tavily_analysis",           tavily_analysis_node)
builder.add_node("reuters_analysis",          reuters_analysis_node)
builder.add_node("stocktwits_analysis",       stocktwits_analysis_node)
builder.add_node("edgar_analysis",            edgar_analysis_node)
builder.add_node("research_reducer",          research_reducer_node)
builder.add_node("analyst_supervisor",        analyst_supervisor_node)
builder.add_node("yahoo",                     yahoo_finance_node)
builder.add_node("kpi",                       kpi_calculator_node)
builder.add_node("chart_gen",                 chart_analysis_node)
builder.add_node("kpi_analysis",              kpi_analysis_node)
builder.add_node("risk_signal",               risk_signal_node)
builder.add_node("profit_loss_expectation",   profit_loss_expectation_node)   # NEW
builder.add_node("analyst_reducer",           analyst_reducer_node)
builder.add_node("analyst_gate",              analyst_gate_node)
builder.add_node("writer_gate",               writer_gate_node)
builder.add_node("writer",                    writer_node)
builder.add_node("image_planner",             image_planner_node)
builder.add_node("gemini_image",              gemini_image_node)

# ── entry ───────────────────────────────────────────────────────────────
builder.set_entry_point("orchestrator")

# orchestrator → both branch supervisors (parallel start)
builder.add_edge("orchestrator", "research_supervisor")
builder.add_edge("orchestrator", "analyst_supervisor")

# ── RESEARCH BRANCH ─────────────────────────────────────────────────────
builder.add_edge("research_supervisor", "tavily")
builder.add_edge("research_supervisor", "reuters_marketwatch")
builder.add_edge("research_supervisor", "stocktwits")
builder.add_edge("research_supervisor", "edgar")

builder.add_edge("tavily",              "tavily_analysis")
builder.add_edge("reuters_marketwatch", "reuters_analysis")
builder.add_edge("stocktwits",          "stocktwits_analysis")
builder.add_edge("edgar",               "edgar_analysis")

for node in ["tavily_analysis", "reuters_analysis",
             "stocktwits_analysis", "edgar_analysis"]:
    builder.add_edge(node, "research_reducer")

builder.add_edge("research_reducer", "writer_gate")

# ── ANALYST BRANCH ──────────────────────────────────────────────────────
builder.add_edge("analyst_supervisor", "yahoo")
builder.add_edge("analyst_supervisor", "kpi")
builder.add_edge("analyst_supervisor", "chart_gen")

# kpi → kpi_analysis → risk_signal → profit_loss_expectation (serial chain)
builder.add_edge("kpi",                      "kpi_analysis")
builder.add_edge("kpi_analysis",             "risk_signal")
builder.add_edge("risk_signal",              "profit_loss_expectation")   # NEW step

# All parallel outputs converge on analyst_gate
builder.add_edge("yahoo",                    "analyst_gate")
builder.add_edge("chart_gen",                "analyst_gate")
builder.add_edge("profit_loss_expectation",  "analyst_gate")              # replaces risk_signal→gate

builder.add_edge("analyst_gate",     "analyst_reducer")
builder.add_edge("analyst_reducer",  "writer_gate")

# ── WRITER GATE → single writer invocation ──────────────────────────────
builder.add_edge("writer_gate",   "writer")

# ── FINAL LINEAR CHAIN ──────────────────────────────────────────────────
builder.add_edge("writer",        "image_planner")
builder.add_edge("image_planner", "gemini_image")
builder.add_edge("gemini_image",  END)

graph = builder.compile(checkpointer=MemorySaver())
logger.success("Graph compiled — 24 nodes ✓  (profit_loss_expectation node added)")

# ─────────────────────────────────────────────────────────────────────────
# Main — NVIDIA hardcoded as default
# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    validate_environment()
    start_ws_server()

    # ── DEFAULT: NVIDIA deep dive ─────────────────────────────────────────
    QUERY = DEFAULT_QUERY   # "NVIDIA stock deep dive analysis"

    initial_state: AgentState = {
        "query":                    QUERY,
        "ticker":                   None,
        "parsed_query":             None,
        "tavily_citations":         [],
        "reuters_marketwatch_data": None,
        "edgar_data":               None,
        "stocktwits_data":          None,
        "yahoo_historical":         None,
        "yahoo_live":               None,
        "kpi_report":               None,
        "charts":                   [],
        "tavily_analysis":          None,
        "reuters_analysis":         None,
        "stocktwits_analysis":      None,
        "edgar_analysis":           None,
        "kpi_analysis":             None,
        "research_merged":          None,
        "analyst_merged":           None,
        "draft_report_markdown":    None,
        "md_with_placeholders":     None,
        "image_specs":              None,
        "final_report_markdown":    None,
        "final_report_pdf":         None,
        "confidence_score":         None,
        "section_scores":           None,
        "risk_signal":              None,
        "risk_reason":              None,
        "profit_loss_expectation":  None,   # NEW
        "retry_count":              0,
        "max_retries":              2,
        "errors":                   [],
    }

    run_id = uuid.uuid4().hex[:8]
    config = {"configurable": {"thread_id": f"research-{run_id}"}}
    logger.info(f"Run ID: {run_id}")

    print("\n🚀  DumaX $50K Research Agent — v4  (NVIDIA Default)\n" + "─"*50)
    print("  Nodes: tavily + reuters + stocktwits + edgar + yahoo + kpi + charts + pnl")
    print("  Flow:  research → analyst → pnl_expectation → writer → image_planner → gemini → done")
    print(f"  Query: {QUERY}\n")

    for event in graph.stream(initial_state, config):
        node_name = list(event.keys())[0]
        print(f"  ✅  {node_name}")

    final    = graph.get_state(config).values
    report   = final.get("final_report_markdown")
    pdf_path = final.get("final_report_pdf")
    score    = final.get("confidence_score", 0) or 0
    signal   = final.get("risk_signal","N/A")
    sec_sc   = final.get("section_scores",{})
    ticker   = final.get("ticker","STOCK")

    print("\n" + "─"*50)
    print(f"  Run ID       : {run_id}")
    print(f"  Ticker       : {ticker}")
    print(f"  Confidence   : {score*100:.1f}%")
    print(f"  Risk Signal  : {signal}")
    if sec_sc:
        print(f"  Sect. Scores : {sec_sc}")
    if final.get("errors"):
        print(f"  Errors       : {final['errors']}")
    if report:
        print(f"\n  📄  Markdown : reports/{ticker}_final_report.md")
    if pdf_path:
        print(f"  📑  PDF      : {pdf_path}")
    print("\n  Done ✓\n")
