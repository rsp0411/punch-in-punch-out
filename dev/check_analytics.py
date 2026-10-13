"""
Local self-check (NOT part of the submission). Needs a real MongoDB 6+.

    MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_check python dev/check_analytics.py

It DROPS the target database, seeds awkward data directly into MongoDB (like the grader does), then compares
every analytics endpoint with an independent plain-Python reference, and asserts the explain plans.
"""
import os, sys, json, random
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from math import floor

os.environ.setdefault("MONGO_DB", "attendance_check")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi.testclient import TestClient
from app import main as m

IST = m.IST
rhu = lambda x, p: floor(Fraction(x) * 10**p + Fraction(1, 2)) / 10**p
PRES = {"PRESENT", "WFH", "ON_DUTY"}

m.client.drop_database(m.MONGO_DB)
m.ensure_indexes()
random.seed(7)
emps = [
    ("EMP0001", "Eng", "2026-01-05"), ("EMP0002", "Eng", "2026-07-13"),   # mid-month joiner
    ("EMP0003", "Sales", "2026-01-05"), ("EMP0004", "Sales", "2026-01-05"),
    ("EMP0005", "Eng", "2026-01-05"), ("EMP0006", "Ops", "2026-01-05"),   # EMP0006: no logs at all
    ("EMP0007", "Ops", "2026-08-01"),                                     # joins after the month
]
m.db.employees.insert_many([{"emp_code": c, "name": f"Name {c}", "email": f"{c}@x.io", "department": d,
    "shift_start": "09:30", "shift_end": "18:30", "joined_on": j, "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    for c, d, j in emps])
logs = []
def add(code, day, status, late=0, ot=0, wh=None, half=False, pin_h=9):
    pi = datetime(day.year, day.month, day.day, pin_h, 30, tzinfo=IST).astimezone(timezone.utc) if status in PRES else None
    logs.append({"emp_code": code, "date": day.isoformat(), "status": status, "punch_in": pi,
                 "punch_out": None if wh is None else pi + timedelta(hours=wh), "work_hours": wh,
                 "late_minutes": late, "overtime_minutes": ot, "half_day": half, "history": []})
d0 = date(2026, 7, 1)
for code, _, joined in emps[:5]:
    for i in range(31):
        day = d0 + timedelta(days=i)
        if day.isoformat() < joined or random.random() < 0.15:     # some days simply missing
            continue
        if day.weekday() >= 5 and random.random() < 0.8:
            continue                                               # a few weekend records survive
        s = random.choice(["PRESENT"] * 6 + ["WFH", "ON_DUTY", "ABSENT", "LEAVE"])
        if s in PRES:
            wh = random.choice([3.5, 4.49, 4.5, 8.0, 9.12, 8.125]); add(code, day, s, random.choice([0, 0, 15, 20, 20, 35]), random.choice([0, 40]), wh, wh < 4.5)
        else:
            add(code, day, s)
add("EMP0003", date(2026, 7, 4), "PRESENT", 20, 0, 8.0)             # Saturday records feed late totals
logs.append({**logs[0], "emp_code": "EMP9999", "date": "2026-07-06", "late_minutes": 500})  # log of unknown employee
# force a tie at the cutoff
for code, mins in (("EMP0001", 40), ("EMP0003", 40), ("EMP0004", 40)):
    logs = [l for l in logs if not (l["emp_code"] == code and l["date"] == "2026-07-31")]
    add(code, date(2026, 7, 31), "PRESENT", mins, 0, 8.0)
m.db.attendance_logs.insert_many(logs)

c = TestClient(m.app)
E = {e[0]: e for e in emps}
def month_logs(month): return [l for l in m.db.attendance_logs.find({"date": {"$gte": month + "-01", "$lte": month + "-31"}})]
wk = lambda s: date.fromisoformat(s).weekday() < 5
units = lambda l: (1 if l.get("half_day") else 2) if l["status"] in PRES and wk(l["date"]) else 0
ok = True
def check(name, got, want):
    global ok
    if got != want:
        ok = False; print("MISMATCH", name, "\n  got ", json.dumps(got)[:600], "\n  want", json.dumps(want)[:600])

M = "2026-07"; first, last = date(2026, 7, 1), date(2026, 7, 31)
# monthly
for code in E:
    ls = [l for l in month_logs(M) if l["emp_code"] == code]
    start = max(first, date.fromisoformat(E[code][2]))
    wd = 0 if start > last else sum(1 for i in range((last - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)
    u = sum(units(l) for l in ls)
    want = {"emp_code": code, "month": M, "working_days": wd, "present_days": u / 2,
            "leave_days": sum(l["status"] == "LEAVE" for l in ls), "late_count": sum(l["late_minutes"] > 0 for l in ls),
            "total_late_minutes": sum(l["late_minutes"] for l in ls), "total_overtime_minutes": sum(l["overtime_minutes"] for l in ls),
            "attendance_pct": rhu(Fraction(u * 100, 2 * wd), 2) if wd else None}
    check("monthly " + code, c.get(f"/analytics/employees/{code}/monthly", params={"month": M}).json(), want)
# department summary
items = []
for dep in sorted({e[1] for e in emps}):
    codes = [e[0] for e in emps if e[1] == dep and e[2] <= "2026-07-31"]
    if not codes: continue
    ls = [l for l in month_logs(M) if l["emp_code"] in codes]
    whs = [round(l["work_hours"] * 100) for l in ls if l["status"] in PRES and l.get("work_hours") is not None]
    items.append({"department": dep, "headcount": len(codes), "present_days": sum(units(l) for l in ls) / 2,
        "avg_work_hours": rhu(Fraction(sum(whs), 100 * len(whs)), 2) if whs else None,
        "late_count": sum(l["late_minutes"] > 0 for l in ls), "total_late_minutes": sum(l["late_minutes"] for l in ls),
        "leave_count": sum(l["status"] == "LEAVE" for l in ls), "on_duty_count": sum(l["status"] == "ON_DUTY" for l in ls)})
check("dept summary", c.get("/analytics/departments/summary", params={"month": M}).json(), {"month": M, "items": items})
check("dept summary filtered", c.get("/analytics/departments/summary", params={"month": M, "department": "Ops"}).json()["items"], [i for i in items if i["department"] == "Ops"])
# leaderboard
def board(limit, dep=None):
    tot = {}
    for l in month_logs(M):
        e = E.get(l["emp_code"])
        if e and l["late_minutes"] > 0 and (dep is None or e[1] == dep):
            t = tot.setdefault(l["emp_code"], [0, 0]); t[0] += l["late_minutes"]; t[1] += 1
    rows = sorted(tot.items(), key=lambda kv: (-kv[1][0], kv[0])); out = []
    for i, (code, (mins, n)) in enumerate(rows):
        rank = 1 + sum(1 for _, (m2, _) in rows if m2 > mins)
        if rank <= limit:
            out.append({"rank": rank, "emp_code": code, "name": f"Name {code}", "department": E[code][1], "total_late_minutes": mins, "late_count": n})
    return out
for lim, dep in ((1, None), (2, None), (3, None), (10, None), (2, "Eng"), (5, "Sales")):
    p = {"month": M, "limit": lim, **({"department": dep} if dep else {})}
    check(f"leaderboard {lim} {dep}", c.get("/analytics/leaderboard/late", params=p).json()["items"], board(lim, dep))
# trend: one row per day, rate on working days only, moving average over the rounded rates inside the range
def trend(dep, frm, to):
    codes = [e for e in emps if e[1] == dep]; days = []
    d = frm
    while d <= to: days.append(d); d += timedelta(days=1)
    rows, rates = [], []
    for d in days:
        ls = [l for l in m.db.attendance_logs.find({"date": d.isoformat()}) if l["emp_code"] in {e[0] for e in codes}]
        hc = sum(1 for e in codes if e[2] <= d.isoformat()); u = sum((1 if l.get("half_day") else 2) for l in ls if l["status"] in PRES)
        rate = rhu(Fraction(u, 2 * hc), 4) if d.weekday() < 5 and hc else None
        rates.append(rate); win = [r for r in rates[-7:] if r is not None]
        rows.append({"date": d.isoformat(), "is_working_day": d.weekday() < 5, "headcount": hc, "present_count": u / 2,
                     "late_count": sum(l["late_minutes"] > 0 for l in ls), "attendance_rate": rate,
                     "moving_avg_7d": rhu(sum(Fraction(str(r)) for r in win) / len(win), 4) if win else None})
    return rows
for dep, a, b in (("Eng", date(2026, 7, 1), date(2026, 7, 31)), ("Sales", date(2026, 6, 20), date(2026, 7, 10)), ("Ops", date(2026, 7, 6), date(2026, 7, 12))):
    got = c.get(f"/analytics/departments/{dep}/trend", params={"from": a.isoformat(), "to": b.isoformat()}).json()["items"]
    check(f"trend {dep}", got, trend(dep, a, b))
# explain: every plan needs an index scan and no collection scan
for ep, p in (("attendance_list", {}), ("attendance_list", {"emp_code": "EMP0001", "status": "PRESENT"}), ("attendance_list", {"date_from": "2026-07-01", "date_to": "2026-07-10"}),
              ("employee_monthly", {"emp_code": "EMP0001", "month": M}), ("department_summary", {"month": M}), ("department_summary", {"month": M, "department": "Eng"}),
              ("late_leaderboard", {"month": M}), ("late_leaderboard", {"month": M, "department": "Eng"}),
              ("department_trend", {"department": "Eng", "from": "2026-07-01", "to": "2026-07-31"})):
    r = c.get(f"/admin/explain/{ep}", params=p); txt = json.dumps(r.json())
    good = r.status_code == 200 and "IXSCAN" in txt and "COLLSCAN" not in txt
    ok &= good; print(("ok   " if good else "FAIL ") + f"explain {ep} {p}")
print("ALL MATCH" if ok else "SOME CHECKS FAILED")
