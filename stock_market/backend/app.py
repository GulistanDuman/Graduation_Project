import asyncio
import base64
import json
import os
import re
import sqlite3
import threading
import uuid
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = Path(__file__).resolve().parent
DB_PATH = BACKEND_DIR / "runtime" / "stock_market_runtime.sqlite3"

load_dotenv(ROOT_DIR / ".env")

import stock_agent_core as agent  # noqa: E402

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = ImageDraw = ImageFont = None

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image as RLImage
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
    from reportlab.platypus import Table, TableStyle
    from xml.sax.saxutils import escape as xml_escape
except Exception:
    colors = A4 = ParagraphStyle = getSampleStyleSheet = cm = RLImage = Paragraph = SimpleDocTemplate = Spacer = Table = TableStyle = xml_escape = None

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin@1234"

app = FastAPI(title="DumaX Stock Agents API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

loop_ref: Optional[asyncio.AbstractEventLoop] = None
thread_ctx = threading.local()
session_queues: Dict[str, List[asyncio.Queue]] = {}
session_nodes: Dict[str, Dict[str, Dict[str, str]]] = {}
session_logs: Dict[str, List[Dict[str, Any]]] = {}
session_meta: Dict[str, Dict[str, Any]] = {}
DB_LOCK = threading.RLock()

ALL_NODES = [
    "orchestrator", "research_supervisor", "analyst_supervisor", "tavily",
    "reuters_marketwatch", "stocktwits", "edgar", "tavily_analysis",
    "reuters_analysis", "stocktwits_analysis", "edgar_analysis",
    "research_reducer", "yahoo", "kpi", "chart_gen", "kpi_analysis",
    "risk_signal", "profit_loss_expectation", "analyst_reducer",
    "analyst_gate", "writer_gate", "writer", "image_planner", "gemini_image",
]


class LoginRequest(BaseModel):
    email: str
    password: str


class SignupRequest(BaseModel):
    fullName: str
    email: str
    password: str


class AnalyzeRequest(BaseModel):
    query: str


def db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with DB_LOCK:
        try:
            _create_schema()
        except (sqlite3.DatabaseError, OSError):
            stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            if DB_PATH.exists():
                DB_PATH.replace(DB_PATH.with_suffix(f".corrupt.{stamp}.sqlite3"))
            journal = DB_PATH.with_name(f"{DB_PATH.name}-journal")
            if journal.exists():
                journal.replace(DB_PATH.with_name(f"{journal.name}.corrupt.{stamp}"))
            _create_schema()


def _create_schema() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                query TEXT NOT NULL,
                ticker TEXT,
                status TEXT NOT NULL,
                confidence REAL,
                risk_signal TEXT,
                section_scores TEXT,
                report_md TEXT,
                pdf_path TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS node_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                node_name TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT,
                ts TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS app_checkpoints (
                session_id TEXT PRIMARY KEY,
                checkpoint_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )


def now() -> str:
    return datetime.utcnow().isoformat()


def save_checkpoint(session_id: str, payload: Dict[str, Any]) -> None:
    session_meta.setdefault(session_id, {})["checkpoint"] = payload


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def clean_report_markdown(report_md: str) -> str:
    lines = []
    previous_blank = False
    for raw in (report_md or "").splitlines():
        line = raw.rstrip()
        if line.strip() in {"---", "____", "_____", "______"}:
            if not previous_blank:
                lines.append("")
                previous_blank = True
            continue
        if re.fullmatch(r"[-_]{3,}", line.strip()):
            if not previous_blank:
                lines.append("")
                previous_blank = True
            continue
        if not line.strip():
            if not previous_blank:
                lines.append("")
                previous_blank = True
            continue
        lines.append(line)
        previous_blank = False
    return "\n".join(lines).strip() + "\n"


def format_pdf_markdown(report_md: str) -> str:
    cleaned = clean_report_markdown(report_md)
    formatted: List[str] = []

    for raw in cleaned.splitlines():
        line = raw.strip()
        if not line:
            formatted.append("")
            continue
        if line.startswith("!["):
            formatted.append(line)
            continue

        line = re.sub(r"\[\(([^)]*?)\)\]\((https?://[^\s)]+)(?:\s+\".*?\")?\)", r"[\1]", line)
        line = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)(?:\s+\".*?\")?\)", r"\1", line)
        line = re.sub(r"\*\*(Technical Analysis|Investment Thesis|Final Verdict|Key Levels|Current Trend|Support|Resistance|Momentum|Risk Signal|Recommendation):\*\*", r"\1:", line)
        line = re.sub(r"(?<!^)\s+(\d+)[.)]\s+([A-Z][A-Za-z0-9 /&,+-]{2,55}:)", r"\n\1) \2", line)
        line = re.sub(r"^(Technical Analysis:)\s+(\d+)[.)]\s+", r"\1\n\2) ", line)

        parts = [part.strip() for part in line.split("\n")]
        formatted.extend(part for part in parts if part)

    return "\n".join(formatted)


def _rl_inline(text: str) -> str:
    text = xml_escape(text)
    link_pattern = re.compile(r"\[\(([^)]*?)\)\]\((https?://[^\s)]+)(?:\s+&quot;.*?&quot;)?\)")
    text = link_pattern.sub(r'<a href="\2" color="blue">(\1)</a>', text)
    text = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)(?:\s+&quot;.*?&quot;)?\)", r'<a href="\2" color="blue">\1</a>', text)
    text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"\*(.*?)\*", r"<i>\1</i>", text)
    return text


def generate_reportlab_pdf(report_md: str, ticker: str, session_id: str) -> Optional[Path]:
    if not SimpleDocTemplate:
        return None

    out_dir = BACKEND_DIR / "generated_pdfs"
    out_dir.mkdir(exist_ok=True)
    pdf_path = out_dir / f"{ticker or 'STOCK'}_{session_id[:8]}_report.pdf"

    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "BodyClient",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.2,
        leading=14.5,
        textColor=colors.HexColor("#17212b"),
        spaceAfter=7,
    )
    h1 = ParagraphStyle(
        "H1Client",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=22,
        leading=27,
        textColor=colors.HexColor("#087c72"),
        spaceAfter=14,
    )
    h2 = ParagraphStyle(
        "H2Client",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=20,
        textColor=colors.HexColor("#087c72"),
        spaceBefore=12,
        spaceAfter=9,
    )
    h3 = ParagraphStyle(
        "H3Client",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=12.5,
        leading=16,
        textColor=colors.HexColor("#087c72"),
        spaceBefore=8,
        spaceAfter=6,
    )
    item = ParagraphStyle(
        "ItemClient",
        parent=body,
        fontName="Helvetica-Bold",
        leftIndent=14,
        textColor=colors.HexColor("#17212b"),
    )

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        rightMargin=1.6 * cm,
        leftMargin=1.6 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
    )
    story = []
    formatted = format_pdf_markdown(report_md)

    for raw in formatted.splitlines():
        line = raw.strip()
        if not line:
            story.append(Spacer(1, 0.12 * cm))
            continue

        img_match = re.match(r"!\[(.*?)\]\((data:image/[^;]+;base64,.*?)\)", line)
        if img_match:
            try:
                raw_img = base64.b64decode(img_match.group(2).split(",", 1)[1])
                img_file = BytesIO(raw_img)
                pil = Image.open(BytesIO(raw_img)) if Image else None
                max_w, max_h = 16.2 * cm, 8.8 * cm
                if pil:
                    scale = min(max_w / pil.width, max_h / pil.height, 1)
                    story.append(RLImage(img_file, width=pil.width * scale, height=pil.height * scale))
                else:
                    story.append(RLImage(img_file, width=max_w, height=max_h))
                caption = xml_escape(img_match.group(1))
                if caption:
                    story.append(Paragraph(f'<font color="#66737d">{caption}</font>', body))
                story.append(Spacer(1, 0.18 * cm))
            except Exception:
                story.append(Paragraph("[Image could not be embedded]", body))
            continue

        if line.startswith("# "):
            story.append(Paragraph(_rl_inline(line[2:].strip()), h1))
        elif line.startswith("## "):
            story.append(Paragraph(_rl_inline(line[3:].strip()), h2))
        elif line.startswith("### "):
            story.append(Paragraph(_rl_inline(line[4:].strip()), h3))
        elif re.fullmatch(r"[A-Z][A-Za-z0-9 /&,+-]{2,60}:", re.sub(r"\*\*", "", line)):
            story.append(Paragraph(_rl_inline(re.sub(r"\*\*", "", line)), h3))
        elif re.match(r"^\d+[.)]\s+[A-Z][A-Za-z0-9 /&,+-]{2,60}:", re.sub(r"\*\*", "", line)):
            story.append(Paragraph(_rl_inline(re.sub(r"\*\*", "", line)), item))
        elif line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if cells and not all(set(c) <= {"-"} for c in cells):
                table = Table([[Paragraph(_rl_inline(c), body) for c in cells]], colWidths=None)
                table.setStyle(TableStyle([
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d9e1e5")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f7fbfb")),
                ]))
                story.append(table)
        else:
            story.append(Paragraph(_rl_inline(line), body))

    doc.build(story)
    return pdf_path


def _font(size: int, bold: bool = False):
    if not ImageFont:
        return None
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _wrap_draw_text(draw, text: str, font, max_width: int) -> List[str]:
    words = text.split()
    if not words:
        return [""]
    lines, current = [], ""
    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width or not current:
            current = test
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def generate_visual_pdf(report_md: str, ticker: str, session_id: str) -> Optional[Path]:
    if not Image:
        return None

    out_dir = BACKEND_DIR / "generated_pdfs"
    out_dir.mkdir(exist_ok=True)
    pdf_path = out_dir / f"{ticker or 'STOCK'}_{session_id[:8]}_report.pdf"

    width, height = 1240, 1754
    margin = 90
    content_width = width - (margin * 2)
    bg = "white"
    ink = "#17212b"
    muted = "#66737d"
    accent = "#087c72"

    title_font = _font(46, True)
    h2_font = _font(32, True)
    h3_font = _font(25, True)
    body_font = _font(22)
    body_bold_font = _font(22, True)
    small_font = _font(18)

    pages = []
    page = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(page)
    y = margin

    def new_page():
        nonlocal page, draw, y
        pages.append(page)
        page = Image.new("RGB", (width, height), bg)
        draw = ImageDraw.Draw(page)
        y = margin

    def ensure(space: int):
        if y + space > height - margin:
            new_page()

    def draw_text_block(text: str, font, fill=ink, gap=10, indent=0):
        nonlocal y
        lines = _wrap_draw_text(draw, text, font, content_width - indent)
        line_h = font.size + 9
        ensure((len(lines) * line_h) + gap)
        for line in lines:
            draw.text((margin + indent, y), line, font=font, fill=fill)
            y += line_h
        y += gap

    def draw_image_from_markdown(line: str) -> bool:
        nonlocal y
        match = re.match(r"!\[(.*?)\]\((data:image/[^;]+;base64,.*?)\)", line.strip())
        if not match:
            return False
        caption, data_url = match.groups()
        try:
            raw = base64.b64decode(data_url.split(",", 1)[1])
            img = Image.open(BytesIO(raw)).convert("RGB")
        except Exception:
            draw_text_block("[Image could not be decoded]", small_font, muted)
            return True

        max_img_h = 520
        scale = min(content_width / img.width, max_img_h / img.height, 1.0)
        size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
        img = img.resize(size)
        ensure(size[1] + 70)
        x = margin + (content_width - size[0]) // 2
        draw.rounded_rectangle((x - 8, y - 8, x + size[0] + 8, y + size[1] + 8), radius=10, outline="#d9e1e5", width=2)
        page.paste(img, (x, y))
        y += size[1] + 14
        if caption:
            draw_text_block(caption, small_font, muted, gap=20)
        return True

    cleaned = format_pdf_markdown(report_md)
    for raw in cleaned.splitlines():
        line = raw.strip()
        if not line:
            y += 12
            continue
        if draw_image_from_markdown(line):
            continue
        if line.startswith("# "):
            ensure(90)
            draw_text_block(line[2:].strip(), title_font, accent, gap=22)
        elif line.startswith("## "):
            ensure(70)
            y += 10
            draw.line((margin, y, width - margin, y), fill="#dce5e8", width=2)
            y += 18
            draw_text_block(line[3:].strip(), h2_font, accent, gap=16)
        elif line.startswith("### "):
            draw_text_block(line[4:].strip(), h3_font, "#24404a", gap=12)
        elif re.fullmatch(r"[A-Z][A-Za-z0-9 /&,+-]{2,60}:", re.sub(r"\*\*", "", line)):
            heading = re.sub(r"\*\*", "", line)
            draw_text_block(heading, h3_font, accent, gap=10)
        elif re.match(r"^\d+[.)]\s+[A-Z][A-Za-z0-9 /&,+-]{2,60}:", re.sub(r"\*\*", "", line)):
            item = re.sub(r"\*\*", "", line)
            draw_text_block(item, body_bold_font, ink, gap=8, indent=26)
        elif line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if cells and not all(set(c) <= {"-"} for c in cells):
                draw_text_block("  |  ".join(cells), small_font, "#1e343b", gap=8)
        else:
            citation_fill = "#155ec7" if re.search(r"\([A-Za-z][A-Za-z ]*,\s*(?:n\.d\.|\d{4})\)", line) else ink
            line = re.sub(r"\*\*(.*?)\*\*", r"\1", line)
            line = re.sub(r"\*(.*?)\*", r"\1", line)
            draw_text_block(line, body_font, citation_fill, gap=13)

    pages.append(page)
    pages[0].save(pdf_path, "PDF", resolution=120.0, save_all=True, append_images=pages[1:])
    return pdf_path


def generate_basic_pdf(report_md: str, ticker: str, session_id: str) -> Path:
    out_dir = BACKEND_DIR / "generated_pdfs"
    out_dir.mkdir(exist_ok=True)
    pdf_path = out_dir / f"{ticker or 'STOCK'}_{session_id[:8]}_report.pdf"

    clean_lines: List[str] = []
    for line in clean_report_markdown(report_md).splitlines():
        if "data:image/" in line:
            clean_lines.append("[Chart/Image available in the web report]")
            continue
        line = line.replace("**", "").replace("###", "").replace("##", "").replace("#", "")
        line = line.strip()
        if line:
            clean_lines.append(line)
        else:
            clean_lines.append("")

    wrapped: List[str] = []
    for line in clean_lines:
        if not line:
            wrapped.append("")
            continue
        while len(line) > 92:
            split_at = line.rfind(" ", 0, 92)
            if split_at < 45:
                split_at = 92
            wrapped.append(line[:split_at].strip())
            line = line[split_at:].strip()
        wrapped.append(line)

    lines_per_page = 46
    pages = [wrapped[i:i + lines_per_page] for i in range(0, len(wrapped), lines_per_page)] or [["No report content."]]

    object_map: Dict[int, str] = {}
    catalog_id = 1
    pages_id = 2
    font_id = 3
    page_ids: List[int] = []
    content_ids: List[int] = []

    next_id = 4
    for page_lines in pages:
        page_ids.append(next_id)
        content_ids.append(next_id + 1)
        next_id += 2

        content = ["BT", "/F1 10 Tf", "50 790 Td", "14 TL"]
        for idx, line in enumerate(page_lines):
            if idx:
                content.append("T*")
            content.append(f"({_pdf_escape(line[:140])}) Tj")
        content.append("ET")
        stream = "\n".join(content)
        object_map[content_ids[-1]] = (
            f"{content_ids[-1]} 0 obj\n<< /Length {len(stream.encode('latin-1', errors='ignore'))} >>\nstream\n{stream}\nendstream\nendobj\n"
        )

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    object_map[catalog_id] = f"{catalog_id} 0 obj\n<< /Type /Catalog /Pages {pages_id} 0 R >>\nendobj\n"
    object_map[pages_id] = f"{pages_id} 0 obj\n<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>\nendobj\n"
    object_map[font_id] = f"{font_id} 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"

    for page_id, content_id in zip(page_ids, content_ids):
        object_map[page_id] = (
            f"{page_id} 0 obj\n<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 595 842] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>\nendobj\n"
        )

    pdf = "%PDF-1.4\n"
    offsets = {0: 0}
    max_id = max(object_map)
    for obj_id in range(1, max_id + 1):
        obj = object_map[obj_id]
        offsets[obj_id] = len(pdf.encode("latin-1", errors="ignore"))
        pdf += obj
    xref_offset = len(pdf.encode("latin-1", errors="ignore"))
    pdf += f"xref\n0 {max_id + 1}\n0000000000 65535 f \n"
    for obj_id in range(1, max_id + 1):
        pdf += f"{offsets[obj_id]:010d} 00000 n \n"
    pdf += f"trailer\n<< /Size {max_id + 1} /Root {catalog_id} 0 R >>\nstartxref\n{xref_offset}\n%%EOF"

    pdf_path.write_bytes(pdf.encode("latin-1", errors="ignore"))
    return pdf_path


def write_node(session_id: str, node: str, status: str, detail: str = "") -> Dict[str, Any]:
    ts = now()
    payload = {
        "type": "node_update",
        "session_id": session_id,
        "node": node,
        "status": status,
        "detail": detail or "",
        "ts": ts,
    }
    session_nodes.setdefault(session_id, {})[node] = {"status": status, "detail": detail or ""}
    session_logs.setdefault(session_id, []).append({
        "node_name": node,
        "status": status,
        "detail": detail or "",
        "ts": ts,
    })
    broadcast(session_id, payload)
    save_checkpoint(session_id, {"nodes": session_nodes.get(session_id, {}), "last": payload})
    return payload


def mark_graph_event(session_id: str, node: str) -> None:
    current = session_nodes.setdefault(session_id, {}).get(node)
    if not current or current.get("status") == "waiting":
        write_node(session_id, node, "done", "completed")


def broadcast(session_id: str, payload: Dict[str, Any]) -> None:
    if not loop_ref:
        return
    queues = list(session_queues.get(session_id, []))
    for q in queues:
        loop_ref.call_soon_threadsafe(q.put_nowait, payload)


def patched_emit_progress(node_name: str, status: str = "running", extra: str = "") -> None:
    session_id = getattr(thread_ctx, "session_id", None)
    if session_id:
        write_node(session_id, node_name, status, extra)
    agent.logger.info(f"[{status.upper()}] {node_name} {extra}")


agent.emit_progress = patched_emit_progress


def parse_ticker_from_query(query: str) -> Optional[str]:
    original = query or ""
    lower = original.lower()
    for company, symbol in getattr(agent, "COMPANY_TICKER_MAP", {}).items():
        if re.search(rf"\b{re.escape(company)}\b", lower):
            return symbol

    explicit = re.search(r"(?:ticker|symbol)\s*[:=]?\s*([A-Z]{1,5})\b", original, re.IGNORECASE)
    if explicit:
        return explicit.group(1).upper()

    labeled = re.search(r"\b(?:stock|shares)\s*[:=]\s*([A-Z]{1,5})\b", original, re.IGNORECASE)
    if labeled:
        return labeled.group(1).upper()

    stopwords = {"WITH", "YEAR", "ONE", "STOCK", "SHARE", "SHARES", "BUY", "SELL", "HOLD"}
    guessed = re.search(r"\b([A-Z]{1,5})\b", original)
    if guessed and guessed.group(1).upper() not in stopwords:
        return guessed.group(1).upper()
    return None


def initial_state(query: str) -> Dict[str, Any]:
    parsed_ticker = parse_ticker_from_query(query)
    return {
        "query": query,
        "ticker": parsed_ticker,
        "parsed_query": query,
        "tavily_citations": [],
        "reuters_marketwatch_data": None,
        "edgar_data": None,
        "stocktwits_data": None,
        "yahoo_historical": None,
        "yahoo_live": None,
        "kpi_report": None,
        "charts": [],
        "tavily_analysis": None,
        "reuters_analysis": None,
        "stocktwits_analysis": None,
        "edgar_analysis": None,
        "kpi_analysis": None,
        "research_merged": None,
        "analyst_merged": None,
        "draft_report_markdown": None,
        "md_with_placeholders": None,
        "image_specs": None,
        "final_report_markdown": None,
        "final_report_pdf": None,
        "confidence_score": None,
        "section_scores": None,
        "risk_signal": None,
        "risk_reason": None,
        "profit_loss_expectation": None,
        "retry_count": 0,
        "max_retries": 2,
        "errors": [],
    }


def persisted_nodes(session_id: str) -> Dict[str, Dict[str, str]]:
    if session_id in session_nodes:
        return session_nodes[session_id]

    nodes = {node: {"status": "waiting", "detail": ""} for node in ALL_NODES}
    try:
        with DB_LOCK:
            with db() as conn:
                session = conn.execute(
                    "SELECT status FROM sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                rows = conn.execute(
                    "SELECT node_name, status, detail FROM node_logs WHERE session_id=? ORDER BY id ASC",
                    (session_id,),
                ).fetchall()
                cp = conn.execute(
                    "SELECT checkpoint_json FROM app_checkpoints WHERE session_id=?",
                    (session_id,),
                ).fetchone()
    except Exception:
        session = None
        rows = []
        cp = None

    if cp:
        try:
            checkpoint_nodes = json.loads(cp["checkpoint_json"]).get("nodes", {})
            for node, payload in checkpoint_nodes.items():
                if node in nodes:
                    nodes[node] = {
                        "status": payload.get("status", "waiting"),
                        "detail": payload.get("detail", ""),
                    }
        except Exception:
            pass

    for row in rows:
        nodes[row["node_name"]] = {"status": row["status"], "detail": row["detail"] or ""}

    if session and session["status"] == "complete":
        for node, payload in nodes.items():
            if payload["status"] == "waiting":
                nodes[node] = {"status": "done", "detail": "completed"}

    session_nodes[session_id] = nodes
    return nodes


def run_agent(session_id: str, query: str) -> None:
    thread_ctx.session_id = session_id
    config = {"configurable": {"thread_id": f"stock-market-{session_id}"}}
    try:
        session_meta.setdefault(session_id, {}).update({"status": "running", "updated_at": now()})

        for node in ALL_NODES:
            session_nodes.setdefault(session_id, {})[node] = {"status": "waiting", "detail": ""}

        for event in agent.graph.stream(initial_state(query), config):
            node_name = list(event.keys())[0]
            mark_graph_event(session_id, node_name)
            save_checkpoint(session_id, {"last_graph_event": node_name, "nodes": session_nodes.get(session_id, {})})

        final = agent.graph.get_state(config).values
        report = final.get("final_report_markdown") or ""
        ticker = final.get("ticker") or "STOCK"
        confidence = float(final.get("confidence_score") or 0)
        risk_signal = final.get("risk_signal") or "N/A"
        section_scores = final.get("section_scores") or {}
        pdf_path = final.get("final_report_pdf")

        finished_at = now()
        session_meta.setdefault(session_id, {}).update({
            "status": "complete",
            "ticker": ticker,
            "confidence": confidence,
            "risk_signal": risk_signal,
            "section_scores": section_scores,
            "report_md": report,
            "pdf_path": pdf_path,
            "updated_at": finished_at,
        })
        try:
            with DB_LOCK:
                with db() as conn:
                    conn.execute(
                        """
                        UPDATE sessions
                        SET status=?, ticker=?, confidence=?, risk_signal=?, section_scores=?,
                            report_md=?, pdf_path=?, updated_at=?
                        WHERE session_id=?
                        """,
                        (
                            "complete", ticker, confidence, risk_signal,
                            json.dumps(section_scores), report, pdf_path, finished_at, session_id,
                        ),
                    )
        except Exception:
            pass

        for node_name, node_state in session_nodes.get(session_id, {}).items():
            if node_state.get("status") == "running":
                write_node(session_id, node_name, "done", node_state.get("detail", "") or "completed")

        broadcast(session_id, {
            "type": "complete",
            "session_id": session_id,
            "ticker": ticker,
            "confidence": confidence,
            "risk_signal": risk_signal,
            "ts": now(),
        })
    except Exception as exc:
        session_meta.setdefault(session_id, {}).update({"status": "failed", "error": str(exc), "updated_at": now()})
        try:
            with DB_LOCK:
                with db() as conn:
                    conn.execute(
                        "UPDATE sessions SET status=?, error=?, updated_at=? WHERE session_id=?",
                        ("failed", str(exc), now(), session_id),
                    )
        except Exception:
            pass
        broadcast(session_id, {"type": "error", "session_id": session_id, "error": str(exc), "ts": now()})
    finally:
        thread_ctx.session_id = None


@app.on_event("startup")
async def startup() -> None:
    global loop_ref
    loop_ref = asyncio.get_running_loop()
    try:
        init_db()
    except Exception:
        # Live analysis uses memory first; SQLite is only persistence/checkpoint storage.
        pass


@app.get("/")
async def root() -> Dict[str, str]:
    return {"status": "ok", "service": "DumaX Stock Agents API"}


@app.post("/api/login")
async def login(req: LoginRequest) -> Dict[str, str]:
    if req.email == ADMIN_USERNAME and req.password == ADMIN_PASSWORD:
        return {"token": "admin-local-token", "role": "admin"}
    raise HTTPException(status_code=401, detail="Invalid admin credentials")


@app.post("/auth/signup")
async def signup(_: SignupRequest) -> Dict[str, Any]:
    return {"enabled": False, "message": "Signup is disabled. Use admin / admin@1234."}


@app.post("/analyze")
async def analyze(req: AnalyzeRequest) -> Dict[str, str]:
    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")

    session_id = uuid.uuid4().hex
    created = now()
    session_meta[session_id] = {
        "session_id": session_id,
        "query": query,
        "status": "queued",
        "ticker": None,
        "confidence": None,
        "risk_signal": None,
        "error": None,
        "created_at": created,
        "updated_at": created,
    }
    try:
        with DB_LOCK:
            with db() as conn:
                conn.execute(
                    """
                    INSERT INTO sessions(session_id, query, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (session_id, query, "queued", created, created),
                )
    except Exception:
        pass

    thread = threading.Thread(target=run_agent, args=(session_id, query), daemon=True)
    thread.start()
    return {"session_id": session_id, "status": "queued"}


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(ws: WebSocket, session_id: str) -> None:
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue()
    session_queues.setdefault(session_id, []).append(q)
    await ws.send_json({"type": "current_status", "session_id": session_id, "nodes": session_nodes.get(session_id, {})})
    try:
        while True:
            payload = await q.get()
            await ws.send_json(payload)
    except WebSocketDisconnect:
        pass
    finally:
        session_queues.get(session_id, []).remove(q)


@app.get("/status/{session_id}")
async def status(session_id: str) -> Dict[str, Any]:
    if session_id in session_meta:
        meta = session_meta[session_id]
        return {
            "session_id": session_id,
            "status": meta.get("status"),
            "ticker": meta.get("ticker"),
            "confidence": meta.get("confidence"),
            "risk_signal": meta.get("risk_signal"),
            "error": meta.get("error"),
            "nodes": persisted_nodes(session_id),
        }
    with DB_LOCK:
        with db() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "session_id": session_id,
        "status": row["status"],
        "ticker": row["ticker"],
        "confidence": row["confidence"],
        "risk_signal": row["risk_signal"],
        "error": row["error"],
        "nodes": persisted_nodes(session_id),
    }


@app.get("/report/{session_id}")
async def report(session_id: str) -> Dict[str, Any]:
    if session_id in session_meta:
        meta = session_meta[session_id]
        return {
            "session_id": session_id,
            "status": meta.get("status"),
            "ticker": meta.get("ticker"),
            "confidence": meta.get("confidence"),
            "risk_signal": meta.get("risk_signal"),
            "section_scores": meta.get("section_scores") or {},
            "report_md": clean_report_markdown(meta.get("report_md") or ""),
        }
    try:
        with DB_LOCK:
            with db() as conn:
                row = conn.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
    except Exception:
        row = None
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "session_id": session_id,
        "status": row["status"],
        "ticker": row["ticker"],
        "confidence": row["confidence"],
        "risk_signal": row["risk_signal"],
        "section_scores": json.loads(row["section_scores"] or "{}"),
        "report_md": clean_report_markdown(row["report_md"] or ""),
    }


@app.get("/report/{session_id}/markdown")
async def report_markdown(session_id: str) -> PlainTextResponse:
    data = await report(session_id)
    return PlainTextResponse(clean_report_markdown(data["report_md"]), media_type="text/markdown")


@app.get("/report/{session_id}/pdf")
async def report_pdf(session_id: str):
    if session_id in session_meta:
        meta = session_meta[session_id]
        row = {
            "ticker": meta.get("ticker"),
            "report_md": meta.get("report_md"),
            "pdf_path": meta.get("pdf_path"),
        }
    else:
        try:
            with DB_LOCK:
                with db() as conn:
                    row = conn.execute(
                        "SELECT ticker, report_md, pdf_path FROM sessions WHERE session_id=?",
                        (session_id,),
                    ).fetchone()
        except Exception:
            row = None
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")

    pdf_path = Path(row["pdf_path"]) if row["pdf_path"] else None
    if row["report_md"]:
        generated_pdf = generate_reportlab_pdf(row["report_md"], row["ticker"] or "STOCK", session_id)
        if not generated_pdf:
            generated_pdf = generate_visual_pdf(row["report_md"], row["ticker"] or "STOCK", session_id)
        if generated_pdf:
            pdf_path = generated_pdf
            session_meta.setdefault(session_id, {})["pdf_path"] = str(pdf_path)
            try:
                with DB_LOCK:
                    with db() as conn:
                        conn.execute(
                            "UPDATE sessions SET pdf_path=?, updated_at=? WHERE session_id=?",
                            (str(pdf_path), now(), session_id),
                        )
            except Exception:
                pass

    if not pdf_path and row["report_md"]:
        generated = agent.export_pdf(row["report_md"], row["ticker"] or "STOCK")
        if not generated:
            visual = generate_visual_pdf(row["report_md"], row["ticker"] or "STOCK", session_id)
            generated = str(visual) if visual else None
        if not generated:
            generated = str(generate_basic_pdf(row["report_md"], row["ticker"] or "STOCK", session_id))
        if generated:
            pdf_path = Path(generated)
            session_meta.setdefault(session_id, {})["pdf_path"] = str(pdf_path)
            try:
                with DB_LOCK:
                    with db() as conn:
                        conn.execute(
                            "UPDATE sessions SET pdf_path=?, updated_at=? WHERE session_id=?",
                            (str(pdf_path), now(), session_id),
                        )
            except Exception:
                pass

    if not pdf_path:
        raise HTTPException(status_code=404, detail="PDF not available yet")
    if not pdf_path.is_absolute():
        pdf_path = ROOT_DIR / pdf_path
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF file not found")
    return FileResponse(pdf_path, media_type="application/pdf", filename=pdf_path.name)


@app.get("/history")
async def history(limit: int = 20) -> Dict[str, Any]:
    memory_rows = sorted(session_meta.values(), key=lambda x: x.get("created_at", ""), reverse=True)
    with DB_LOCK:
        try:
            with db() as conn:
                rows = conn.execute(
                    """
                    SELECT session_id, query, ticker, status, confidence, risk_signal, created_at, updated_at
                    FROM sessions
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            db_rows = [dict(row) for row in rows]
        except Exception:
            db_rows = []
    merged = {row["session_id"]: row for row in db_rows}
    for row in memory_rows:
        merged[row["session_id"]] = {
            "session_id": row["session_id"],
            "query": row["query"],
            "ticker": row.get("ticker"),
            "status": row.get("status"),
            "confidence": row.get("confidence"),
            "risk_signal": row.get("risk_signal"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }
    return {"results": list(merged.values())[:limit]}


@app.delete("/history/{session_id}")
async def delete_history(session_id: str) -> Dict[str, bool]:
    try:
        with DB_LOCK:
            with db() as conn:
                conn.execute("DELETE FROM node_logs WHERE session_id=?", (session_id,))
                conn.execute("DELETE FROM app_checkpoints WHERE session_id=?", (session_id,))
                conn.execute("DELETE FROM sessions WHERE session_id=?", (session_id,))
    except Exception:
        pass
    session_nodes.pop(session_id, None)
    session_logs.pop(session_id, None)
    session_meta.pop(session_id, None)
    return {"deleted": True}


@app.delete("/history")
async def clear_history() -> Dict[str, bool]:
    try:
        with DB_LOCK:
            with db() as conn:
                conn.execute("DELETE FROM node_logs")
                conn.execute("DELETE FROM app_checkpoints")
                conn.execute("DELETE FROM sessions")
    except Exception:
        pass
    session_nodes.clear()
    session_logs.clear()
    session_meta.clear()
    return {"deleted": True}


@app.get("/nodes/{session_id}")
async def nodes(session_id: str) -> Dict[str, Any]:
    if session_id in session_logs:
        return {"nodes": session_logs[session_id]}
    try:
        with DB_LOCK:
            with db() as conn:
                rows = conn.execute(
                    "SELECT node_name, status, detail, ts FROM node_logs WHERE session_id=? ORDER BY id ASC",
                    (session_id,),
                ).fetchall()
    except Exception:
        rows = []
    if rows:
        return {"nodes": [dict(row) for row in rows]}
    node_map = persisted_nodes(session_id)
    return {
        "nodes": [
            {"node_name": node, "status": payload["status"], "detail": payload["detail"], "ts": now()}
            for node, payload in node_map.items()
            if payload["status"] != "waiting"
        ]
    }


@app.get("/frontend")
async def frontend_hint() -> HTMLResponse:
    return HTMLResponse("<p>Open ../frontend/index.html or serve the frontend folder on port 3000.</p>")
