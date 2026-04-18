# main.py changes for Phase C

Open `main.py` and apply these three additions. Do NOT remove anything;
we're purely adding alongside the existing loyalty routers.

---

## 1. At the top of the file, with the other `from ... import` statements

Add:

```python
# ---- Accounting core (Phase C) ----
from core.api.errors import register_error_handlers
from core.api.vouchers_router import router as core_vouchers_router
from core.api.settlement_router import router as core_settlement_router
from core.api.masters_router import router as core_masters_router
```

---

## 2. After the `app = FastAPI(...)` line and after CORS is configured

(i.e., after all existing `app.include_router(...)` calls for the loyalty
routers — auth, admin, contractors, inventory, reports, sync.)

Add:

```python
# ---- Accounting core (Phase C) ----
# Register the DomainError -> { code, message, details } envelope handler
# before including the routers so 400/404/409 responses are consistent.
register_error_handlers(app)

app.include_router(core_masters_router)
app.include_router(core_vouchers_router)
app.include_router(core_settlement_router)
```

---

## 3. If you want a quick sanity print on startup (optional)

Inside the existing startup event handler (if any), or add one:

```python
@app.on_event("startup")
def _log_routes():
    import logging
    log = logging.getLogger("srpc_api")
    core_routes = [
        r.path for r in app.routes
        if getattr(r, "path", "").startswith("/api/v1/")
    ]
    log.info("Accounting core routes registered: %d", len(core_routes))
```

---

## After applying the patch

```bash
# Test that the file still parses
python -c "import ast; ast.parse(open('main.py').read()); print('OK')"

# Restart
sudo systemctl restart srpc_api

# Check status
sudo systemctl status srpc_api --no-pager

# Verify new routes are live
curl -s https://api.saraswatiretail.com/openapi.json | \
    python -c "import json,sys; d=json.load(sys.stdin); \
print('\n'.join(sorted(p for p in d['paths'] if p.startswith('/api/v1/'))))"
```

You should see:

```
/api/v1/masters/ledgers
/api/v1/masters/parties
/api/v1/masters/parties/{party_id}
/api/v1/settlement/allocate
/api/v1/settlement/bills/opening
/api/v1/settlement/outstanding
/api/v1/vouchers/contra
/api/v1/vouchers/journal
/api/v1/vouchers/payment
/api/v1/vouchers/receipt
/api/v1/vouchers/{voucher_id}
/api/v1/vouchers/{voucher_id}/cancel
```
