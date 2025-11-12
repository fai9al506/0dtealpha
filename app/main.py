from fastapi import FastAPI
from datetime import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()
last_run_status = {"ts": None, "ok": False, "msg": "boot"}

def run_market_job():
    last_run_status.update({
        "ts": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "ok": True,
        "msg": "market job ran"
    })

@app.get("/")
def health():
    return {"status": "ok", "last": last_run_status}

@app.get("/api/snapshot")
def snapshot():
    return {"example": True, "note": "replace with real snapshot"}

def start_scheduler():
    sch = BackgroundScheduler(timezone="US/Eastern")
    sch.add_job(
        run_market_job,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*", timezone="US/Eastern")
    )
    sch.start()

@app.on_event("startup")
def on_startup():
    start_scheduler()
