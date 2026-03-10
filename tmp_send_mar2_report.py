"""One-shot: generate and send March 2 daily chart + PDF to Telegram."""
import os
from datetime import date
from sqlalchemy import create_engine
from app.eod_report import generate_trades_chart, send_telegram_photo, generate_eod_pdf, send_telegram_pdf

DB_URL = os.getenv("DATABASE_URL", "")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_SETUPS", "") or os.getenv("TELEGRAM_CHAT_ID", "")
trade_date = date(2026, 3, 2)

engine = create_engine(DB_URL)
date_str = "March 02, 2026"

# 1. Trades chart
chart_path = generate_trades_chart(engine, trade_date)
if chart_path:
    send_telegram_photo(chart_path, f"0DTE Alpha — {date_str}", BOT_TOKEN, CHAT_ID)
    try: os.unlink(chart_path)
    except: pass

# 2. PDF report
pdf_path = generate_eod_pdf(engine, trade_date)
if pdf_path:
    send_telegram_pdf(pdf_path, f"0DTE Alpha Daily Report - {date_str}", BOT_TOKEN, CHAT_ID)
    try: os.unlink(pdf_path)
    except: pass

print("Done!")
