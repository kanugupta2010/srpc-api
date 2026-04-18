# Phase C — Deployment & Testing

## 1. Deploy

On your laptop, in your local clone of srpc-api:

```bash
cd ~/path/to/srpc-api

# Extract the Phase C bundle (overlays on top of Phase A+B)
tar -xzf ~/Downloads/srpc_phase_c.tar.gz --strip-components=1 -C .

# Review what changed
git status
# Expected:
#   modified:   core/db.py                             (Phase B fix — own pool)
#   modified:   tests/conftest.py                      (teardown for new tables)
#   modified:   tests/test_static_org_scope.py         (new tenant tables)
#   new file:   alembic/versions/0006_phase_c_settlement.py
#   new file:   core/schemas/__init__.py
#   new file:   core/schemas/voucher_schemas.py
#   new file:   core/schemas/settlement_schemas.py
#   new file:   core/schemas/party_schemas.py
#   new file:   core/repos/bill_references.py
#   new file:   core/repos/allocations.py
#   new file:   core/services/settlement_service.py
#   new file:   core/services/party_service.py
#   new file:   core/services/voucher_cancel_service.py
#   new file:   core/api/__init__.py
#   new file:   core/api/deps.py
#   new file:   core/api/errors.py
#   new file:   core/api/vouchers_router.py
#   new file:   core/api/settlement_router.py
#   new file:   core/api/masters_router.py
#   new file:   tests/test_settlement_service.py
#   new file:   tests/test_voucher_cancel_service.py
#   new file:   tests/test_party_service.py
#   new file:   MAIN_PY_PATCH.md

# Apply the main.py patch by hand — open main.py in your editor and follow
# the three steps in MAIN_PY_PATCH.md. Then:
python -c "import ast; ast.parse(open('main.py').read()); print('OK')"

# Commit
git add .
git commit -m "Phase C: Payment/Receipt/Journal/Contra + Settlement + Parties"
git push origin main
```

On the droplet:

```bash
ssh srpcconnect
cd /home/srpc/srpc_api
source venv/bin/activate
git pull

# Load env vars
export $(grep -v '^#' .env | xargs)

# Apply migration 0006
alembic upgrade head
alembic current    # should print 0006 (head)

# Restart API (new routers get registered)
sudo systemctl restart srpc_api
sudo systemctl status srpc_api --no-pager

# Verify new endpoints live
curl -s https://api.saraswatiretail.com/openapi.json | \
    python -c "import json,sys; d=json.load(sys.stdin); \
print('\n'.join(sorted(p for p in d['paths'] if p.startswith('/api/v1/'))))"
```

Expected output:
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

## 2. Smoke test (curl)

First get an admin JWT — use your existing admin login:

```bash
TOKEN=$(curl -s -X POST https://api.saraswatiretail.com/auth/admin/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"<your-admin-password>"}' \
    | python -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

echo "$TOKEN" | head -c 40  # sanity
```

### 2.1 List ledgers (the 4 starter ledgers from seed_phase_a)

```bash
curl -s https://api.saraswatiretail.com/api/v1/masters/ledgers \
    -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

### 2.2 Create a customer (with opening balance)

```bash
curl -s -X POST https://api.saraswatiretail.com/api/v1/masters/parties \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{
        "party_type": "CUSTOMER",
        "name": "ABC Traders",
        "mobile": "9876543210",
        "gstin": "07ABCDE1234F1Z5",
        "state_code": "07",
        "opening_balance": "5000.00",
        "opening_balance_dr_cr": "Dr"
    }' | python -m json.tool
```

Note the returned `ledger_id` — call it `$CUSTOMER_LEDGER_ID`.

### 2.3 Post a Journal voucher (opening cash balance)

```bash
# Find your Cash-in-Hand ledger id
CASH_LEDGER_ID=$(curl -s "https://api.saraswatiretail.com/api/v1/masters/ledgers?group_name=Cash-in-Hand" \
    -H "Authorization: Bearer $TOKEN" | python -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")

# Capital ledger — you'll need to create it first via a journal, or via a
# future ledger-creation endpoint (not built yet). For now, use any
# non-cash/bank ledger to balance the entry.
SALES_LEDGER_ID=$(curl -s "https://api.saraswatiretail.com/api/v1/masters/ledgers?group_name=Sales%20Accounts" \
    -H "Authorization: Bearer $TOKEN" | python -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")

curl -s -X POST https://api.saraswatiretail.com/api/v1/vouchers/journal \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{
        \"voucher_date\": \"2025-05-01\",
        \"narration\": \"Cash opening\",
        \"lines\": [
            {\"ledger_id\": $CASH_LEDGER_ID,  \"dr_cr\": \"Dr\", \"amount\": \"50000.00\"},
            {\"ledger_id\": $SALES_LEDGER_ID, \"dr_cr\": \"Cr\", \"amount\": \"50000.00\"}
        ]
    }" | python -m json.tool
```

Returns `{"voucher_id": X, "voucher_number": "JV0001", ...}`.

### 2.4 Post a Payment voucher

Pay ₹15,000 rent from HDFC Bank. First you'd need a "Rent" expense ledger,
which doesn't exist in the seed. Skip for now or create it via a journal
entry to a new ledger — next phase will add a ledger creation endpoint.

For now, a simple cash-to-bank Contra works:

### 2.5 Contra: Deposit cash into bank

```bash
BANK_LEDGER_ID=$(curl -s "https://api.saraswatiretail.com/api/v1/masters/ledgers?group_name=Bank%20Accounts" \
    -H "Authorization: Bearer $TOKEN" | python -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")

curl -s -X POST https://api.saraswatiretail.com/api/v1/vouchers/contra \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{
        \"voucher_date\": \"2025-05-02\",
        \"from_ledger_id\": $CASH_LEDGER_ID,
        \"to_ledger_id\": $BANK_LEDGER_ID,
        \"amount\": \"20000.00\",
        \"narration\": \"Cash deposit\"
    }" | python -m json.tool
```

### 2.6 Full flow: opening bill, receipt, allocation

Create an opening bill for ABC Traders (they owed you ₹3000 from pre-system days):

```bash
# Use the $CUSTOMER_LEDGER_ID from step 2.2
curl -s -X POST https://api.saraswatiretail.com/api/v1/settlement/bills/opening \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{
        \"party_ledger_id\": $CUSTOMER_LEDGER_ID,
        \"bill_no\": \"SI-OLD-001\",
        \"bill_date\": \"2025-03-15\",
        \"amount\": \"3000.00\",
        \"side\": \"RECEIVABLE\",
        \"notes\": \"Outstanding from Busy migration\"
    }"
```

Returns `{"bill_reference_id": X}` — call it `$BILL_ID`.

Customer pays ₹3000 — post a Receipt and allocate in one call:

```bash
curl -s -X POST https://api.saraswatiretail.com/api/v1/vouchers/receipt \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{
        \"voucher_date\": \"2025-05-10\",
        \"received_into_ledger_id\": $BANK_LEDGER_ID,
        \"party_ledger_id\": $CUSTOMER_LEDGER_ID,
        \"lines\": [
            {\"ledger_id\": $CUSTOMER_LEDGER_ID, \"amount\": \"3000.00\"}
        ],
        \"allocate_to_bills\": [
            {\"bill_reference_id\": $BILL_ID, \"amount\": \"3000.00\"}
        ],
        \"narration\": \"Payment received\"
    }" | python -m json.tool
```

Verify the bill cleared:

```bash
curl -s "https://api.saraswatiretail.com/api/v1/settlement/outstanding?party_ledger_id=$CUSTOMER_LEDGER_ID" \
    -H "Authorization: Bearer $TOKEN" | python -m json.tool
# Empty array — bill is now CLEARED, not returned.
```

### 2.7 Cancel a voucher (reverses allocations)

```bash
# Using the receipt voucher id from step 2.6
curl -s -X POST https://api.saraswatiretail.com/api/v1/vouchers/$RECEIPT_ID/cancel \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"reason": "Cheque bounced"}' | python -m json.tool
```

Check the bill re-opens:

```bash
curl -s "https://api.saraswatiretail.com/api/v1/settlement/outstanding?party_ledger_id=$CUSTOMER_LEDGER_ID" \
    -H "Authorization: Bearer $TOKEN" | python -m json.tool
# The bill is back: status=OPEN, outstanding_amount=3000.00
```

## 3. Run tests (on the droplet, against dev MySQL)

These tests create data in organization_id=9999. **Run against a dev/staging
DB, not the live contractorconnect.** If you don't have a separate dev DB yet,
skip this section — the smoke tests above are enough to validate.

```bash
cd /home/srpc/srpc_api
source venv/bin/activate
export $(grep -v '^#' .env.dev | xargs)    # dev DB env vars
pytest tests/ -v
```

Expected: ~30 tests pass. The static org-scope scanner is the most
important one — a failure there means new code got merged without
filtering by organization_id, and Phase D onwards must not ship until
it's green.

## 4. Rollback (if anything goes wrong)

```bash
# On droplet
ssh srpcconnect
cd /home/srpc/srpc_api
source venv/bin/activate
export $(grep -v '^#' .env | xargs)

# Revert DB schema (drops bill_references, allocations, audit_log)
alembic downgrade 0005

# Revert code
git revert HEAD
git push

# Restart
sudo systemctl restart srpc_api
```

The rollback is safe because Phase C only added new tables and new
endpoints — nothing in the loyalty program path reads or writes them.
