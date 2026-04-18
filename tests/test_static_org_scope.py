"""Static analysis: fail the build if any SQL in core/ hits a
tenant-scoped table without filtering by organization_id.

How it works
------------
1. Walk every .py file under core/.
2. Parse with `ast`; collect every string literal (including Python
   f-strings in their raw, pre-interpolation form).
3. Parse each string with `sqlparse` to see if it's SQL.
4. For every SELECT / UPDATE / DELETE / INSERT statement, check:
       * Does it reference a tenant-scoped table?
       * If SELECT/UPDATE/DELETE — does it filter by organization_id?
       * If INSERT — does the column list include organization_id?
5. If any violation is found, fail with a list of (file, line, stmt).

What's enforced
---------------
Tenant-scoped tables = those listed in `TENANT_TABLES` below. Add new
ones here when Phase C/D/E introduces them.

What's NOT enforced
-------------------
* Pre-Phase-A code under routers/, services/, etc. — exempted by
  scanning only the `core/` tree.
* Dynamic SQL built by string concatenation at runtime — the scanner
  sees only the literal fragments. Any new code that concatenates SQL
  should prefer a single literal with named params; otherwise the
  scanner can miss issues. This is acceptable because `core/` uses
  named-parameter dicts exclusively by convention (see base repo).
* JOINs where the filter applies to an aliased table rather than the
  bare table name — the check is satisfied if the predicate
  `organization_id = ` appears anywhere in the WHERE clause at all.
  That's intentionally loose; a tighter check would false-flag
  legitimate queries with ledger_alias.organization_id.
"""
from __future__ import annotations

import ast
import os
import re
from pathlib import Path

import pytest
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import DML, Keyword

# Tables that carry organization_id. Keep in sync with migrations.
TENANT_TABLES = frozenset(
    {
        "account_groups",
        "ledgers",
        "parties",
        "financial_years",
        "voucher_series",
        "vouchers",
        "voucher_lines",
    }
)

# Directories scanned. Add to this as new core subpackages are created.
CORE_ROOT = Path(__file__).resolve().parents[1] / "core"
SCRIPTS_ROOT = Path(__file__).resolve().parents[1] / "scripts"
SCANNED_ROOTS = [CORE_ROOT, SCRIPTS_ROOT]


# ---- AST string collection ------------------------------------------------

def _collect_sql_strings(tree: ast.AST) -> list[tuple[int, str]]:
    """Return (lineno, raw_string) for every str constant and f-string."""
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append((node.lineno, node.value))
        elif isinstance(node, ast.JoinedStr):
            # f-strings — concatenate the static parts; ignore FormattedValue.
            pieces = []
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    pieces.append(v.value)
                else:
                    # FormattedValue — replace with a harmless placeholder
                    # so the SQL parser still sees a valid structure.
                    pieces.append(" __PLACEHOLDER__ ")
            out.append((node.lineno, "".join(pieces)))
    return out


# ---- Statement inspection -------------------------------------------------

_SELECT_UPDATE_DELETE = {"SELECT", "UPDATE", "DELETE"}
_INSERT = {"INSERT"}


def _first_dml(stmt: Statement) -> str | None:
    """Return the first DML keyword (SELECT/INSERT/UPDATE/DELETE) or None."""
    for tok in stmt.flatten():
        if tok.ttype in (DML, Keyword.DML):
            val = str(tok).strip().upper()
            if val in _SELECT_UPDATE_DELETE | _INSERT:
                return val
    return None


def _referenced_tables(stmt: Statement) -> set[str]:
    """Heuristic table-name extractor.

    Covers: FROM x, JOIN x, UPDATE x, INSERT INTO x, DELETE FROM x.
    Good enough for our code; not a full SQL parser.
    """
    sql = str(stmt)
    tables: set[str] = set()
    patterns = [
        r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    ]
    for pat in patterns:
        for match in re.finditer(pat, sql, re.IGNORECASE):
            tables.add(match.group(1).lower())
    return tables


def _has_org_predicate(sql: str) -> bool:
    """True if the SQL contains `organization_id` referenced in WHERE/ON."""
    # Either `organization_id = ...` or `organization_id IN (...)`
    # or `.organization_id = ...` (aliased).
    return bool(
        re.search(
            r"\borganization_id\b\s*(=|IN\b)",
            sql,
            re.IGNORECASE,
        )
    )


def _inserts_organization_id(sql: str) -> bool:
    """True if the INSERT's column list includes organization_id."""
    # Look for INSERT INTO <tbl> ( ... organization_id ... ) VALUES ( ... )
    m = re.search(
        r"INSERT\s+INTO\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\(([^)]*)\)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return False
    cols = m.group(1)
    return bool(re.search(r"\borganization_id\b", cols, re.IGNORECASE))


# ---- The test ------------------------------------------------------------

def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SCANNED_ROOTS:
        if not root.exists():
            continue
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.endswith(".py"):
                    files.append(Path(dirpath) / fn)
    return files


def test_no_sql_misses_org_id_on_tenant_tables():
    violations: list[str] = []

    for pyfile in _iter_python_files():
        src = pyfile.read_text(encoding="utf-8")
        try:
            tree = ast.parse(src, filename=str(pyfile))
        except SyntaxError as e:  # pragma: no cover
            pytest.fail(f"{pyfile}: syntax error {e}")

        for lineno, literal in _collect_sql_strings(tree):
            # Cheap filter — skip strings that plainly aren't SQL.
            upper = literal.upper()
            if not any(
                kw in upper
                for kw in ("SELECT ", "INSERT ", "UPDATE ", "DELETE ")
            ):
                continue

            for parsed in sqlparse.parse(literal):
                dml = _first_dml(parsed)
                if dml is None:
                    continue
                tables = _referenced_tables(parsed)
                hit_tenant = tables & TENANT_TABLES
                if not hit_tenant:
                    continue

                sql_text = str(parsed)

                if dml in _SELECT_UPDATE_DELETE:
                    if not _has_org_predicate(sql_text):
                        violations.append(
                            f"{pyfile}:{lineno}: {dml} on "
                            f"{sorted(hit_tenant)} without "
                            f"organization_id predicate\n"
                            f"    {sql_text[:200]}"
                        )
                elif dml in _INSERT:
                    if not _inserts_organization_id(sql_text):
                        violations.append(
                            f"{pyfile}:{lineno}: INSERT into "
                            f"{sorted(hit_tenant)} without "
                            f"organization_id column\n"
                            f"    {sql_text[:200]}"
                        )

    if violations:
        pytest.fail(
            "Tenant-scope violations in core/ or scripts/:\n"
            + "\n".join(violations)
        )
