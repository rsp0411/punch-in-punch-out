"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (real environment variables win over a local .env)

Layout of this single file:
  1. configuration & indexes      4. request / response models
  2. time + business-rule helpers 5. employees / attendance endpoints
  3. record (de)serialisation     6. analytics pipelines + endpoints, explain
"""
import calendar
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from datetime import time as Time
from fractions import Fraction
from math import floor
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration & indexes
# --------------------------------------------------------------------------- #
load_dotenv()  # override=False: real environment variables take priority

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "attendance_db")

# tz_aware=True: every datetime read back is UTC-aware, never naive.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000)
db = client[MONGO_DB]

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: re-creating an index with the same keys/options is a no-op."""
    global _indexes_ready
    emp, logs = db.employees, db.attendance_logs
    # employees
    emp.create_index([("emp_code", 1)], unique=True, name="uq_emp_code")          # identity + race-safe create + $lookup join
    emp.create_index([("department", 1), ("emp_code", 1)], name="ix_dept_emp")    # list by department sorted by emp_code; trend
    emp.create_index([("joined_on", 1)], name="ix_joined_on")                     # headcount (R9) without a department filter
    # attendance_logs
    logs.create_index([("emp_code", 1), ("date", 1)], unique=True, name="uq_emp_date")  # one record per day (race-safe), monthly, lookups
    logs.create_index([("date", -1), ("emp_code", 1)], name="ix_date_emp")        # default list order, date-range scans, leaderboard
    logs.create_index([("status", 1), ("date", -1), ("emp_code", 1)], name="ix_status_date_emp")  # list filtered by status
    logs.create_index([("emp_code", 1), ("punch_in", -1)], name="ix_emp_punch_in")  # "latest punch-in <= t" for punch-out
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # Mongo not up yet: /health will retry
        log.warning("index creation deferred: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

# --------------------------------------------------------------------------- #
# 2. Time + business-rule helpers
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")  # R6

DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
HHMM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"

Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
# strict=True: floats, strings and bools are rejected; the bounds reject "seconds-looking" values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> UTC-aware datetime truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def dt_to_ms(dt: datetime) -> int:
    return (aware(dt) - EPOCH) // timedelta(milliseconds=1)


def ms_or_none(v: Any) -> Any:
    return dt_to_ms(v) if isinstance(v, datetime) else v


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def trunc(dt: datetime) -> datetime:
    return aware(dt).replace(microsecond=0)


def round_half_up(value, places: int) -> float:
    """Exact half-up rounding (no float / banker's-rounding surprises)."""
    scaled = Fraction(value) * 10**places
    return floor(scaled + Fraction(1, 2)) / 10**places


def hhmm(s: str) -> Time:
    h, m = s.split(":")
    return Time(int(h), int(m))


def parse_date(s: str, name: str) -> Date:
    try:
        if len(s) != 10:
            raise ValueError
        return Date.fromisoformat(s)
    except ValueError:
        raise HTTPException(422, f"{name} must be a valid YYYY-MM-DD date")


def month_bounds(month: str) -> tuple[Date, Date]:
    try:
        y, m = int(month[:4]), int(month[5:7])
        return Date(y, m, 1), Date(y, m, calendar.monthrange(y, m)[1])
    except ValueError:
        raise HTTPException(422, "month must be a valid YYYY-MM")


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares correctly as text


def attendance_date_for(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in belongs to the previous day."""
    local = aware(punch_in).astimezone(IST)
    d = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < hhmm(shift_end):
        d -= timedelta(days=1)
    return d.isoformat()


def shift_instant(date_str: str, time_str: str, plus_days: int = 0) -> datetime:
    return datetime.combine(Date.fromisoformat(date_str) + timedelta(days=plus_days), hhmm(time_str), tzinfo=IST)


def compute_late_minutes(punch_in: datetime, date_str: str, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor(minutes since shift start)."""
    secs = int((trunc(punch_in) - shift_instant(date_str, shift_start)).total_seconds())
    return secs // 60 if secs > 600 else 0


def compute_overtime_minutes(punch_out: datetime, date_str: str, shift_start: str, shift_end: str) -> int:
    """R3: floor minutes after shift end (next day for overnight shifts), only if >= 30."""
    plus = 1 if is_overnight(shift_start, shift_end) else 0
    secs = int((trunc(punch_out) - shift_instant(date_str, shift_end, plus)).total_seconds())
    mins = secs // 60
    return mins if mins >= 30 else 0


def work_hours_cents(punch_in: datetime, punch_out: datetime) -> int:
    """R4 in integer hundredths of an hour, half-up: floor(secs*100/3600 + 1/2)."""
    secs = int((trunc(punch_out) - trunc(punch_in)).total_seconds())
    return (secs * 200 + 3600) // 7200


def derive(emp: dict, status: str, date_str: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All derived fields for a record in its final state (R2-R5; table in DATA_MODEL.md)."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    out["late_minutes"] = compute_late_minutes(punch_in, date_str, emp["shift_start"])
    if punch_out is not None:
        cents = work_hours_cents(punch_in, punch_out)
        out["work_hours"] = cents / 100
        out["overtime_minutes"] = compute_overtime_minutes(punch_out, date_str, emp["shift_start"], emp["shift_end"])
        out["half_day"] = cents < 450  # R5: on the rounded value
    return out


def working_days(first: Date, last: Date, joined_on: str) -> int:
    """R7: Mon-Fri days in [first, last], counted from joined_on for mid-month joiners."""
    start = max(first, Date.fromisoformat(joined_on))
    if start > last:
        return 0
    return sum(1 for i in range((last - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)


# --------------------------------------------------------------------------- #
# 3. Serialisation (BSON -> API shape)
# --------------------------------------------------------------------------- #
def employee_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"], "name": d["name"], "email": d["email"], "department": d["department"],
        "shift_start": d["shift_start"], "shift_end": d["shift_end"], "joined_on": d["joined_on"],
        "created_at": dt_to_ms(d["created_at"]) if d.get("created_at") else None,
    }


def history_out(h: dict) -> dict:
    changes = {k: {"from": ms_or_none(v.get("from")), "to": ms_or_none(v.get("to"))}
               for k, v in (h.get("changes") or {}).items()}
    return {"at": ms_or_none(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes}


def record_out(d: dict) -> dict:
    """Never exposes _id; fills the gaps legacy documents may have (missing half_day / history / counters)."""
    return {
        "emp_code": d["emp_code"], "date": d["date"], "status": d["status"],
        "punch_in": ms_or_none(d.get("punch_in")), "punch_out": ms_or_none(d.get("punch_out")),
        "work_hours": d.get("work_hours"),
        "late_minutes": d.get("late_minutes") or 0,
        "overtime_minutes": d.get("overtime_minutes") or 0,
        "half_day": bool(d.get("half_day", False)),
        "history": [history_out(h) for h in (d.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 4. Models
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM_RE)
    shift_end: str = Field("18:30", pattern=HHMM_RE)
    joined_on: str = Field(pattern=DATE_RE)

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        try:
            Date.fromisoformat(v)
        except ValueError:
            raise ValueError("joined_on must be a real calendar date")
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are not declared, so they are ignored if sent.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# 5. Endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not available")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        db.employees.insert_one(doc)  # the unique index on emp_code decides, not a prior find
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {"department": department} if department else {}
    cur = db.employees.find(q, {"_id": 0}).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    total = db.employees.count_documents(q)
    return {"items": [employee_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = db.employees.find_one({"emp_code": body.emp_code}, {"_id": 0})
    if not emp:
        raise HTTPException(404, "employee not found")
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    date_str = attendance_date_for(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code, "date": date_str, "status": body.status,
        "punch_in": ts, "punch_out": None, "work_hours": None,
        "late_minutes": compute_late_minutes(ts, date_str, emp["shift_start"]),
        "overtime_minutes": 0, "half_day": False, "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = db.employees.find_one({"emp_code": body.emp_code}, {"_id": 0})
    if not emp:
        raise HTTPException(404, "employee not found")
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    # Most recent record whose punch_in <= ts. This also finds an overnight shift's record from "yesterday".
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", -1)]
    )
    if not rec:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    pin = aware(rec["punch_in"])
    if ts <= pin:
        raise HTTPException(422, "punched_at must be after punch_in")
    if ts - pin > timedelta(hours=24):
        raise HTTPException(422, "punched_at is more than 24 hours after punch_in")
    d = derive(emp, rec["status"], rec["date"], pin, ts)
    # Conditional update: only one of several simultaneous punch-outs can match punch_out == null.
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": rec["emp_code"], "date": rec["date"], "punch_out": None, "punch_in": rec["punch_in"]},
        {"$set": {"punch_out": ts, "work_hours": d["work_hours"],
                  "overtime_minutes": d["overtime_minutes"], "half_day": d["half_day"]}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def build_attendance_query(emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str],
                           status: Optional[str]) -> dict:
    """Shared by GET /attendance and its explain."""
    df = parse_date(date_from, "date_from") if date_from else None
    dt_ = parse_date(date_to, "date_to") if date_to else None
    if df and dt_ and df > dt_:
        raise HTTPException(422, "date_from must not be after date_to")
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", -1), ("emp_code", 1)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q = build_attendance_query(emp_code, date_from, date_to, status)
    cur = db.attendance_logs.find(q, {"_id": 0}).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    items = [record_out(d) for d in cur]
    total = db.attendance_logs.estimated_document_count() if not q else db.attendance_logs.count_documents(q)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    parse_date(date, "date")
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if not emp:
        raise HTTPException(404, "employee not found")
    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not rec:
        raise HTTPException(404, "no attendance record for that date")

    old_status, old_pi, old_po = rec["status"], aware(rec.get("punch_in")), aware(rec.get("punch_out"))
    new_status = body.status or old_status
    if new_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(422, "ABSENT / LEAVE clear the punch times; do not supply them")
        new_pi = new_po = None
    else:
        new_pi = ms_to_dt(body.punch_in) if body.punch_in is not None else old_pi
        new_po = ms_to_dt(body.punch_out) if body.punch_out is not None else old_po
        if new_pi is None:
            raise HTTPException(422, "a presence status requires a punch_in")
        if attendance_date_for(new_pi, emp["shift_start"], emp["shift_end"]) != date:
            raise HTTPException(422, "punch_in must stay on the record's attendance date")
        if new_po is not None and (new_po <= new_pi or new_po - new_pi > timedelta(hours=24)):
            raise HTTPException(422, "punch_out must be after punch_in and within 24 hours of it")

    if (new_status == old_status and ms_or_none(new_pi) == ms_or_none(old_pi)
            and ms_or_none(new_po) == ms_or_none(old_po)):
        raise HTTPException(422, "the request changes nothing")

    new = {"status": new_status, "punch_in": new_pi, "punch_out": new_po,
           **derive(emp, new_status, date, new_pi, new_po)}
    old = {"status": old_status, "punch_in": old_pi, "punch_out": old_po,
           "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
           "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day", False))}
    changes = {k: {"from": old[k], "to": new[k]} for k in new if ms_or_none(old[k]) != ms_or_none(new[k])}
    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: the write only applies if the record is still exactly as we read it
    # (history length included), so two concurrent corrections cannot silently drop each other's entry.
    n_hist = len(rec.get("history") or [])
    hist_cond = ({"history": {"$size": n_hist}} if n_hist
                 else {"$or": [{"history": {"$exists": False}}, {"history": {"$size": 0}}]})
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": emp_code, "date": date, "status": old_status,
         "punch_in": rec.get("punch_in"), "punch_out": rec.get("punch_out"), **hist_cond},
        {"$set": new, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "the record was modified concurrently; retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 6. Analytics: aggregation pipelines (+ their endpoints), explain
# --------------------------------------------------------------------------- #
# Field-path helper: prefix "$" addresses the current document, "$$l." a $map/$filter variable named l.
PRES_LIST = list(PRESENCE)
WEEKDAYS = [2, 3, 4, 5, 6]  # $dayOfWeek: 1 = Sunday ... 7 = Saturday


def _present(p: str) -> dict:
    return {"$in": [p + "status", PRES_LIST]}


def _is_weekday(date_expr: str) -> dict:
    return {"$in": [{"$dayOfWeek": {"$dateFromString": {"dateString": date_expr}}}, WEEKDAYS]}


def _units(p: str, weekdays_only: bool) -> dict:
    """Presence in half-days: a full present day = 2, a half day = 1, anything else = 0 (R5-R7)."""
    cond = _present(p) if not weekdays_only else {"$and": [_present(p), _is_weekday(p + "date")]}
    return {"$cond": [cond, {"$cond": [{"$eq": [p + "half_day", True]}, 1, 2]}, 0]}


def _late(p: str) -> dict:
    return {"$ifNull": [p + "late_minutes", 0]}


def _is_late(p: str) -> dict:
    return {"$gt": [_late(p), 0]}


def pipeline_employee_monthly(emp_code: str, start: str, end: str) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {"$group": {
            "_id": None,
            "present_units": {"$sum": _units("$", True)},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [_is_late("$"), 1, 0]}},
            "total_late_minutes": {"$sum": _late("$")},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def pipeline_department_summary(start: str, end: str, department: Optional[str]) -> list:
    """Starts from employees (so people with no logs still count) and joins each one's logs for the month."""
    match: dict = {"joined_on": {"$lte": end}}  # R9
    if department:
        match["department"] = department
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {"$project": {"_id": 0, "status": 1, "date": 1, "half_day": 1, "late_minutes": 1, "work_hours": 1}},
            ],
            "as": "logs",
        }},
        {"$addFields": {"wh": {"$filter": {
            "input": "$logs", "as": "l",
            "cond": {"$and": [_present("$$l."), {"$isNumber": "$$l.work_hours"}]}}}}},
        {"$project": {
            "_id": 0, "department": 1,
            "units": {"$sum": {"$map": {"input": "$logs", "as": "l", "in": _units("$$l.", True)}}},
            "wh_n": {"$size": "$wh"},
            # work_hours in integer hundredths so sums are exact; the mean is rounded half-up in Python
            "wh_cents": {"$sum": {"$map": {"input": "$wh", "as": "l",
                                           "in": {"$round": [{"$multiply": ["$$l.work_hours", 100]}, 0]}}}},
            "late_n": {"$size": {"$filter": {"input": "$logs", "as": "l", "cond": _is_late("$$l.")}}},
            "late_total": {"$sum": {"$map": {"input": "$logs", "as": "l", "in": _late("$$l.")}}},
            "leave_n": {"$size": {"$filter": {"input": "$logs", "as": "l",
                                              "cond": {"$eq": ["$$l.status", "LEAVE"]}}}},
            "duty_n": {"$size": {"$filter": {"input": "$logs", "as": "l",
                                             "cond": {"$eq": ["$$l.status", "ON_DUTY"]}}}},
        }},
        {"$group": {
            "_id": "$department", "headcount": {"$sum": 1},
            "units": {"$sum": "$units"}, "wh_n": {"$sum": "$wh_n"}, "wh_cents": {"$sum": "$wh_cents"},
            "late_n": {"$sum": "$late_n"}, "late_total": {"$sum": "$late_total"},
            "leave_n": {"$sum": "$leave_n"}, "duty_n": {"$sum": "$duty_n"},
        }},
        {"$sort": {"_id": 1}},
    ]


def pipeline_late_leaderboard(start: str, end: str, limit: int, department: Optional[str]) -> list:
    p: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code",
                     "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}], "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees drop out here
    ]
    if department:
        p.append({"$match": {"emp.department": department}})  # rank inside the department only
    p += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applied AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
                      "total_late_minutes": 1, "late_count": 1}},
    ]
    return p


def pipeline_department_trend(department: str, frm: Date, to: Date) -> list:
    n_days = (to - frm).days + 1
    first = datetime(frm.year, frm.month, frm.day, tzinfo=UTC)
    return [
        {"$match": {"department": department}},
        {"$group": {"_id": None, "codes": {"$push": "$emp_code"}, "joined": {"$push": "$joined_on"}}},
        # an array localField matches foreignField against any of its elements
        {"$lookup": {
            "from": "attendance_logs", "localField": "codes", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": frm.isoformat(), "$lte": to.isoformat()}}},
                {"$group": {"_id": "$date", "units": {"$sum": _units("$", False)},
                            "late": {"$sum": {"$cond": [_is_late("$"), 1, 0]}}}},
            ],
            "as": "daily",
        }},
        # gap filling: one generated row per calendar day, then each day picks up its (maybe absent) stats
        {"$project": {"joined": 1, "daily": 1, "day": {"$map": {
            "input": {"$range": [0, n_days]}, "as": "i",
            "in": {"$dateToString": {"format": "%Y-%m-%d",
                                     "date": {"$add": [first, {"$multiply": ["$$i", 86_400_000]}]}}}}}}},
        {"$unwind": "$day"},
        {"$project": {
            "_id": 0, "date": "$day",
            "is_working_day": _is_weekday("$day"),
            "headcount": {"$size": {"$filter": {"input": "$joined", "as": "j", "cond": {"$lte": ["$$j", "$day"]}}}},
            "stat": {"$arrayElemAt": [{"$filter": {"input": "$daily", "as": "d",
                                                   "cond": {"$eq": ["$$d._id", "$day"]}}}, 0]},
        }},
        {"$project": {"date": 1, "is_working_day": 1, "headcount": 1,
                      "units": {"$ifNull": ["$stat.units", 0]}, "late_count": {"$ifNull": ["$stat.late", 0]}}},
        # attendance_rate in 1/10000ths, half-up, exactly: rate = units / (2 * headcount)
        {"$addFields": {"rate_q": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [{"$add": [{"$multiply": ["$units", 10000]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]}]}},
            None]}}},
        {"$setWindowFields": {"sortBy": {"date": 1}, "output": {
            "win_sum": {"$sum": "$rate_q", "window": {"documents": [-6, 0]}},
            "win_cnt": {"$sum": {"$cond": [{"$ne": ["$rate_q", None]}, 1, 0]}, "window": {"documents": [-6, 0]}},
        }}},
        {"$project": {
            "date": 1, "is_working_day": 1, "headcount": 1, "late_count": 1,
            "present_count": {"$divide": ["$units", 2]},
            "attendance_rate": {"$cond": [{"$ne": ["$rate_q", None]}, {"$divide": ["$rate_q", 10000]}, None]},
            "moving_avg_7d": {"$cond": [{"$gt": ["$win_cnt", 0]}, {"$divide": [
                {"$floor": {"$divide": [{"$add": [{"$multiply": [2, "$win_sum"]}, "$win_cnt"]},
                                        {"$multiply": [2, "$win_cnt"]}]}}, 10000]}, None]},
        }},
        {"$sort": {"date": 1}},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_RE)):
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0, "joined_on": 1})
    if not emp:
        raise HTTPException(404, "employee not found")
    first, last = month_bounds(month)
    rows = list(db.attendance_logs.aggregate(
        pipeline_employee_monthly(emp_code, first.isoformat(), last.isoformat())))
    a = rows[0] if rows else {}
    units = a.get("present_units", 0)
    wd = working_days(first, last, emp["joined_on"])
    return {
        "emp_code": emp_code, "month": month, "working_days": wd,
        "present_days": round_half_up(Fraction(units, 2), 2),
        "leave_days": a.get("leave_days", 0), "late_count": a.get("late_count", 0),
        "total_late_minutes": a.get("total_late_minutes", 0),
        "total_overtime_minutes": a.get("total_overtime_minutes", 0),
        "attendance_pct": round_half_up(Fraction(units * 100, 2 * wd), 2) if wd else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_RE), department: Optional[str] = None):
    first, last = month_bounds(month)
    rows = db.employees.aggregate(
        pipeline_department_summary(first.isoformat(), last.isoformat(), department), allowDiskUse=True)
    items = []
    for r in rows:
        n = int(r["wh_n"])
        items.append({
            "department": r["_id"], "headcount": r["headcount"],
            "present_days": round_half_up(Fraction(int(r["units"]), 2), 2),
            "avg_work_hours": round_half_up(Fraction(int(round(r["wh_cents"])), 100 * n), 2) if n else None,
            "late_count": r["late_n"], "total_late_minutes": int(r["late_total"]),
            "leave_count": r["leave_n"], "on_duty_count": r["duty_n"],
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_RE),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    first, last = month_bounds(month)
    rows = db.attendance_logs.aggregate(
        pipeline_late_leaderboard(first.isoformat(), last.isoformat(), limit, department), allowDiskUse=True)
    return {"month": month, "items": [
        {**r, "rank": int(r["rank"]), "total_late_minutes": int(r["total_late_minutes"])} for r in rows]}


def validate_trend_range(frm: str, to: str) -> tuple[Date, Date]:
    f, t = parse_date(frm, "from"), parse_date(to, "to")
    if t < f:
        raise HTTPException(422, "to must not be before from")
    if (t - f).days + 1 > 92:
        raise HTTPException(422, "range must not exceed 92 days")
    return f, t


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from", pattern=DATE_RE),
                     to: str = Query(..., pattern=DATE_RE)):
    f, t = validate_trend_range(from_, to)
    if not db.employees.find_one({"department": department}, {"_id": 1}):
        raise HTTPException(404, "unknown department")
    rows = db.employees.aggregate(pipeline_department_trend(department, f, t), allowDiskUse=True)
    return {"department": department, "items": [
        {**r, "headcount": int(r["headcount"]), "late_count": int(r["late_count"]),
         "present_count": float(r["present_count"])} for r in rows]}


# --- explain ---------------------------------------------------------------- #
def _explain(command: dict) -> dict:
    res = db.command({"explain": command, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(res, json_options=json_util.RELAXED_JSON_OPTIONS))


def _need(**params: Any) -> None:
    missing = [k for k, v in params.items() if v is None]
    if missing:
        raise HTTPException(422, "missing required query parameter(s): " + ", ".join(missing))


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                      "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_RE),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from", pattern=DATE_RE),
    to: Optional[str] = Query(None, pattern=DATE_RE),
    page: int = Query(1, ge=1, le=1_000_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    """Builds each query with the same builders the real endpoint uses, then asks MongoDB to explain it."""
    if endpoint == "attendance_list":
        q = build_attendance_query(emp_code, date_from, date_to, status)
        coll = "attendance_logs"
        cmd = {"find": coll, "filter": q, "sort": dict(ATTENDANCE_SORT),
               "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint == "employee_monthly":
            _need(emp_code=emp_code, month=month)
            first, last = month_bounds(month)
            coll, pipe = "attendance_logs", pipeline_employee_monthly(emp_code, first.isoformat(), last.isoformat())
        elif endpoint == "department_summary":
            _need(month=month)
            first, last = month_bounds(month)
            coll, pipe = "employees", pipeline_department_summary(first.isoformat(), last.isoformat(), department)
        elif endpoint == "late_leaderboard":
            _need(month=month)
            first, last = month_bounds(month)
            coll, pipe = "attendance_logs", pipeline_late_leaderboard(
                first.isoformat(), last.isoformat(), limit, department)
        else:  # department_trend
            _need(department=department, **{"from": from_, "to": to})
            f, t = validate_trend_range(from_, to)
            coll, pipe = "employees", pipeline_department_trend(department, f, t)
        cmd = {"aggregate": coll, "pipeline": pipe, "cursor": {}, "allowDiskUse": True}
    return {"endpoint": endpoint, "collection": coll, "explain": _explain(cmd)}
