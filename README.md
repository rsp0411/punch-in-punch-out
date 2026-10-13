# Employee Attendance & Analytics API

FastAPI + MongoDB. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). `GET /health` returns 200 once MongoDB answers.

## Notes
- All endpoints in `openapi.yaml` are implemented, including the five explain targets.
- Analytics are aggregation pipelines. Final rounding is done exactly (half-up) either with integer arithmetic inside the
  pipeline (trend) or in Python on the already-aggregated totals (employee / department summaries), because `$round`
  is banker's rounding.
- Judgement calls where the contract is open: `moving_avg_7d` averages the *reported* (4-decimal) rates; a PATCH whose
  `status`, `punch_in` and `punch_out` all equal the stored values counts as "changes nothing" (422); the trend limit
  of 92 days counts both end dates; for equal `punch_out` / `punch_in` the response is 422.
- `dev/check_analytics.py` is a local self-check (needs a real MongoDB); it is not needed to run the service.
