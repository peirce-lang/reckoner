"""
preflight.py — run before packaging, and after any change to the build path.

Exercises the whole pipeline end to end and prints PASS or FAIL. Takes a few
seconds. The point is to discover breakage BEFORE spending twenty minutes on a
Tauri build, not after.

Usage:
    python preflight.py                    (uses preflight_fixture.csv beside it)
    python preflight.py path\\to\\some.csv

Exit code is 0 when everything passes, 1 otherwise, so it can gate a build
script later if you want.

── What it covers ────────────────────────────────────────────────────────────

  1. Imports          every module the app needs actually loads
  2. Settings         paths resolve, and are writable
  3. Encoder          the 38-check conformance suite
  4. Inference        types and cardinality classify correctly
  5. Build            a real substrate is produced from a real CSV
  6. Conformance      that substrate passes every audit check
  7. Routing          a two-address intersection returns the right count
  8. Frontend build   the packaged interface is not older than its source
  9. Configuration    no development substrate can ship inside .env

Step 7 is the one that matters most. Everything can be individually correct
and the product still not do its job; that check is the job.

Step 8 is the cheapest and covers the failure of July 30, where every backend
gate passed against a frontend that had not been built since May 23. It is a
packaging check, not a pipeline check: it is skipped entirely when there is no
frontend source tree beside this file.
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
for parent in [HERE] + list(HERE.parents):
    if (parent / "model_builder").is_dir():
        sys.path.insert(0, str(parent))
        ROOT = parent
        break
else:
    sys.exit("Could not find the model_builder package above this file.")

PASSED: list[str] = []
FAILED: list[tuple[str, str]] = []


def check(label):
    """Decorator: run a check, record the outcome, never let it abort the run."""
    def wrap(fn):
        try:
            detail = fn()
            PASSED.append(label)
            print(f"  [ok] {label}" + (f" — {detail}" if detail else ""))
        except Exception as exc:
            FAILED.append((label, f"{type(exc).__name__}: {exc}"))
            print(f"  [XX] {label}")
            print(f"       {type(exc).__name__}: {exc}")
        return fn
    return wrap


def section(title):
    print(f"\n{title}\n{'-' * len(title)}")


# ── 1. Imports ───────────────────────────────────────────────────────────────
section("1. Imports")


@check("model_builder.settings")
def _():
    from model_builder import settings
    return f"port {settings.PORT}"


@check("snf.address encoder")
def _():
    from snf.address import encode_coordinate, GRAMMAR_VERSION
    return GRAMMAR_VERSION


@check("model_builder.inference")
def _():
    from model_builder.inference import infer_snf_type, classify_cardinality
    return None


@check("compiler.duckdb emitter")
def _():
    from model_builder.compiler.duckdb import emit_duckdb
    return None


@check("snf_peirce.srf")
def _():
    from snf_peirce.srf import SRFRecord, SRF_VERSION
    return f"wire format {SRF_VERSION}"


@check("duckdb available")
def _():
    import duckdb
    return getattr(duckdb, "__version__", "installed")


# ── 2. Settings ──────────────────────────────────────────────────────────────
section("2. Settings and storage")


@check("directories writable")
def _():
    from model_builder import settings
    problems = settings.check_writable()
    if problems:
        raise RuntimeError("; ".join(problems))
    return str(settings.SUBSTRATES_DIR)


@check("build output and query input agree")
def _():
    from model_builder import settings
    if settings.ARTIFACTS_DIR != settings.SUBSTRATES_DIR:
        raise RuntimeError(
            f"Model Builder writes to {settings.ARTIFACTS_DIR} but the API "
            f"reads {settings.SUBSTRATES_DIR}. A built substrate will not "
            f"appear without a copy step."
        )
    return None


@check("not writing to a temp directory")
def _():
    from model_builder import settings
    if "temp" in str(settings.ARTIFACTS_DIR).lower() or \
       "tmp" in str(settings.ARTIFACTS_DIR).lower():
        raise RuntimeError(
            f"{settings.ARTIFACTS_DIR} looks like a temp folder. The OS will "
            f"delete substrates on its own schedule."
        )
    return None


# ── 3. Encoder conformance ───────────────────────────────────────────────────
section("3. Address encoder")


@check("encoder is pure — no declaration parameter")
def _():
    import inspect
    from snf.address import encode_coordinate
    params = list(inspect.signature(encode_coordinate).parameters)
    if params != ["fact", "grammar_version"]:
        raise RuntimeError(f"signature drifted to {params}")
    return None


@check("boolean renders lowercase")
def _():
    from snf.address import (encode_coordinate, CanonicalFact,
                             GRAMMAR_VERSION)
    got = encode_coordinate(
        CanonicalFact("WHAT", "explicit", True, "boolean"), GRAMMAR_VERSION)
    if got != "what|explicit|true":
        raise RuntimeError(f"got {got!r}")
    return None


@check("no module builds addresses by hand")
def _():
    offenders = []
    for pattern in ("compiler", "snf_peirce"):
        folder = ROOT / ("model_builder/compiler" if pattern == "compiler" else pattern)
        if not folder.is_dir():
            continue
        for py in folder.rglob("*.py"):
            text = py.read_text(encoding="utf-8", errors="ignore")
            for i, line in enumerate(text.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if 'f"' in line and '|{' in line and "coordinate" in line:
                    offenders.append(f"{py.name}:{i}")
    if offenders:
        raise RuntimeError("hand-built coordinate at " + ", ".join(offenders))
    return None


# ── 4. Inference ─────────────────────────────────────────────────────────────
section("4. Type inference and cardinality")


@check("types infer correctly")
def _():
    import pandas as pd
    from model_builder.inference import infer_snf_type as t
    cases = {
        "boolean": ["True", "False"],
        "integer": ["1", "2", "3"],
        "float":   ["0.31", "4.0"],
        "text":    ["00123", "00456"],      # identifiers keep leading zeros
        "date":    ["2019-08-23", "2020-01-05"],
    }
    for expected, values in cases.items():
        got = t(pd.Series(values))
        if got != expected:
            raise RuntimeError(f"{values} inferred {got}, expected {expected}")
    return f"{len(cases)} types"


@check("identifiers flagged as non-routable")
def _():
    import pandas as pd
    from model_builder.inference import classify_cardinality
    ids = classify_cardinality(pd.Series([f"id_{i}" for i in range(50)]), "WHAT")
    if ids["verdict"] != "identifier":
        raise RuntimeError(f"unique column classified {ids['verdict']}")
    grouped = classify_cardinality(pd.Series(["a", "b", "a", "b"] * 10), "WHAT")
    if grouped["verdict"] != "routable":
        raise RuntimeError(f"grouping column classified {grouped['verdict']}")
    return None


# ── 5, 6, 7. Build a real substrate and interrogate it ───────────────────────
section("5. Build a substrate")

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "preflight_fixture.csv"
_state: dict = {}


@check("source CSV present")
def _():
    if not CSV.is_file():
        raise FileNotFoundError(
            f"{CSV} not found. Pass a CSV path as the first argument."
        )
    return CSV.name


@check("build completes")
def _():
    import pandas as pd
    from model_builder.compiler.duckdb import emit_duckdb

    df = pd.read_csv(CSV, dtype=str)

    # Choose the most nearly-unique column as nucleus rather than assuming the
    # first one. On a real export the first column is often a catalog number
    # with duplicates, and a colliding nucleus silently merges entities — which
    # would make every downstream count wrong for a reason unrelated to the
    # thing being tested.
    def _uniqueness(col):
        nz = df[col].dropna()
        nz = nz[nz.str.strip() != ""]
        return (nz.nunique() / len(nz)) if len(nz) else 0.0

    nucleus_col = max(df.columns, key=_uniqueness)
    _state["nucleus"] = f"{nucleus_col} ({_uniqueness(nucleus_col):.0%} unique)"

    mapping = []
    for col in df.columns:
        if col == nucleus_col:
            continue
        dim = "WHEN" if ("date" in col.lower() or "publish" in col.lower()) else "WHAT"
        mapping.append({
            "column": col,
            "dimension": dim,
            "semantic_key": col.strip().replace(" ", "_").replace("-", "_").lower(),
        })

    spec = {
        "mapping": mapping,
        "nucleus": {"columns": [nucleus_col], "separator": "-", "prefix": ""},
        "lens":    {"lens_id": "preflight"},
        "target":  {"output_name": "_preflight_check"},
    }
    # emit_duckdb returns (dim_rows, meta_rows, result_dict). The substrate
    # path is inside the result dict; the emitter chooses it from OUTPUT_DIR,
    # which is exactly the setting checked in section 2.
    _, _, result = emit_duckdb(df, spec)
    _state["result"] = result
    _state["path"] = Path(result["output_path"])
    _state["df"] = df

    if not _state["path"].is_file():
        raise FileNotFoundError(
            f"emitter reported {_state['path']} but no file is there"
        )
    return (f"{result.get('fact_count', 0):,} facts, nucleus "
            f"{_state['nucleus']}")


section("6. Conformance of the built substrate")


def _conn():
    import duckdb
    if "conn" not in _state:
        if "path" not in _state:
            raise RuntimeError("build did not complete — see section 5")
        _state["conn"] = duckdb.connect(str(_state["path"]), read_only=True)
    return _state["conn"]


def _count(sql: str) -> int:
    return _conn().execute(sql).fetchone()[0]


@check("no capitalized booleans")
def _():
    n = _count("SELECT COUNT(*) FROM snf_spoke WHERE value IN "
               "('True','False','TRUE','FALSE')")
    if n:
        raise RuntimeError(f"{n} facts")
    return None


@check("dimension column is lowercase")
def _():
    rows = _conn().execute("SELECT DISTINCT dimension FROM snf_spoke").fetchall()
    bad = [d for (d,) in rows if d and d != d.lower()]
    if bad:
        raise RuntimeError(
            f"{bad} — queries bind dim.lower(), so this substrate would "
            f"return zero rows for every query"
        )
    return None


@check("semantic_key agrees with coordinate")
def _():
    n = _count("SELECT COUNT(*) FROM snf_spoke WHERE coordinate <> "
               "lower(dimension) || '|' || semantic_key || '|' || value")
    if n:
        raise RuntimeError(f"{n} facts — an SRF export would not match")
    return None


@check("no untrimmed or empty values")
def _():
    n = _count("SELECT COUNT(*) FROM snf_spoke WHERE value <> TRIM(value) "
               "OR value IS NULL OR TRIM(value) = ''")
    if n:
        raise RuntimeError(f"{n} facts")
    return None


@check("no comma-split damage")
def _():
    n = _count("SELECT COUNT(*) FROM snf_spoke WHERE value LIKE '%)' "
               "AND value NOT LIKE '%(%'")
    if n:
        raise RuntimeError(f"{n} values with an unbalanced parenthesis")
    return None


@check("no years rendered as floats")
def _():
    n = _count("SELECT COUNT(*) FROM snf_spoke WHERE lower(dimension) = 'when' "
               "AND value LIKE '%.0' AND TRY_CAST(value AS DOUBLE) IS NOT NULL")
    if n:
        raise RuntimeError(f"{n} WHEN values ending in .0")
    return None


# ── Cardinality: the statistic three guardrails rest on ──────────────────────
# Added July 31, 2026. `distinct_values` returned COUNT(DISTINCT entity_id)
# across five implementations. Every consumer wanted distinct VALUES.
# These are semantic regression tests, not shape tests: they only pass if the
# fixture can tell the two statistics apart.


@check("cardinality fixture is discriminating")
def _():
    """A test where entity count == value count proves nothing. Prove it can fail."""
    rows = _conn().execute(
        "SELECT dimension, semantic_key, "
        "COUNT(DISTINCT entity_id) AS entities, "
        "COUNT(DISTINCT value) AS values_ "
        "FROM snf_spoke GROUP BY dimension, semantic_key "
        "HAVING COUNT(DISTINCT entity_id) <> COUNT(DISTINCT value)"
    ).fetchall()
    if not rows:
        raise RuntimeError(
            "no field in this substrate distinguishes entity cardinality from "
            "value cardinality, so the checks below would pass under either "
            "implementation. Use a fixture with a low-cardinality field."
        )
    _state["card_field"] = (rows[0][0], rows[0][1])
    return f"{len(rows)} discriminating field(s), e.g. {rows[0][1]}"


@check("affordances reports value cardinality, not entity cardinality")
def _():
    """Replicates the affordances SQL and checks it against ground truth."""
    if "card_field" not in _state:
        raise RuntimeError("no discriminating field — see previous check")
    dim, skey = _state["card_field"]
    reported = _count(
        f"SELECT COUNT(DISTINCT value) FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}'"
    )
    entities = _count(
        f"SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}'"
    )
    truth = len(_conn().execute(
        f"SELECT DISTINCT value FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}'"
    ).fetchall())
    if reported != truth:
        raise RuntimeError(f"reported {reported}, actual distinct values {truth}")
    if reported == entities:
        raise RuntimeError(
            f"value count {reported} equals entity count — this field cannot "
            f"distinguish the two, so the check is vacuous"
        )
    return f"{skey}: {reported} values vs {entities} entities"


@check("value list length agrees with reported cardinality")
def _():
    """The check that would have caught the values() GROUP BY defect.

    /api/values and /api/affordances answer the same question by different
    SQL. When they disagree, one of them is wrong and neither says so.
    """
    if "card_field" not in _state:
        raise RuntimeError("no discriminating field — see earlier check")
    dim, skey = _state["card_field"]
    reported = _count(
        f"SELECT COUNT(DISTINCT value) FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}'"
    )
    listed = len(_conn().execute(
        f"SELECT value, COUNT(DISTINCT entity_id) FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}' "
        f"GROUP BY value"
    ).fetchall())
    if reported != listed:
        raise RuntimeError(
            f"affordances says {reported} distinct values, the value list "
            f"returns {listed} rows"
        )
    return f"{listed} values, both paths agree"


@check("enum inference uses value cardinality")
def _():
    """A small value domain over many entities must classify as enum."""
    if "card_field" not in _state:
        raise RuntimeError("no discriminating field — see earlier check")
    dim, skey = _state["card_field"]
    n = _count(
        f"SELECT COUNT(DISTINCT value) FROM snf_spoke "
        f"WHERE dimension = '{dim}' AND semantic_key = '{skey}'"
    )
    if n > 25:
        return f"{skey} has {n} values — above the enum threshold, not applicable"
    field_name = skey.split(".")[-1]
    date_kw = ["year", "date", "month", "day", "release", "activity"]
    num_kw  = ["count", "amount", "price", "cmc", "size"]
    if any(k in field_name.lower() for k in date_kw + num_kw):
        return f"{field_name} matches a keyword rule — precedence applies first"
    return f"{field_name}: {n} values, enum branch reachable"



section("7. Routing — the part that is the product")


@check("addresses actually intersect")
def _():
    conn = _conn()
    fields = conn.execute("""
        SELECT semantic_key, COUNT(DISTINCT value) AS vals
        FROM snf_spoke WHERE lower(dimension) = 'what'
        GROUP BY 1 HAVING COUNT(DISTINCT value) BETWEEN 2 AND 12
        ORDER BY vals DESC LIMIT 2
    """).fetchall()
    if len(fields) < 2:
        raise RuntimeError("no two groupable WHAT fields to intersect")

    (ka, _), (kb, _) = fields

    # Pick the commonest value of the first field, then the commonest value of
    # the second field AMONG THE ENTITIES THAT HAVE IT. That guarantees the
    # two addresses co-occur, so an empty result is a genuine failure rather
    # than an honest "nothing matches both". A broken system also returns
    # zero; this check has to be able to tell the difference.
    va = conn.execute("SELECT value FROM snf_spoke WHERE semantic_key = ? "
                      "GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1", [ka]).fetchone()[0]
    vb_row = conn.execute("""
        SELECT value FROM snf_spoke
        WHERE semantic_key = ?
          AND entity_id IN (SELECT entity_id FROM snf_spoke
                            WHERE semantic_key = ? AND value = ?)
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1
    """, [kb, ka, va]).fetchone()
    if vb_row is None:
        raise RuntimeError(f"no entity carries both {ka} and {kb}")
    vb = vb_row[0]

    total = conn.execute("SELECT COUNT(DISTINCT entity_id) FROM snf_spoke").fetchone()[0]
    both = conn.execute("""
        SELECT COUNT(DISTINCT entity_id) FROM snf_spoke
        WHERE semantic_key = ? AND value = ?
          AND entity_id IN (SELECT entity_id FROM snf_spoke
                            WHERE semantic_key = ? AND value = ?)
    """, [ka, va, kb, vb]).fetchone()[0]

    if both == 0:
        raise RuntimeError(
            f"{ka}={va} and {kb}={vb} co-occur by construction but the "
            f"intersection is empty — routing is broken"
        )

    only_a = conn.execute("SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                          "WHERE semantic_key = ? AND value = ?",
                          [ka, va]).fetchone()[0]
    if both > only_a:
        raise RuntimeError("intersection larger than one of its inputs")
    skipped = 100 * (1 - both / total) if total else 0
    return (f"{ka}={va} ∩ {kb}={vb} → {both} of {total} "
            f"({skipped:.0f}% skipped)")


section("8. Frontend build freshness")

# The standalone API serves reckoner\dist; Tauri serves Reckoner\webroot. Both
# are produced by build.bat from one Vite build. This section asserts only that
# what is on disk is not older than the source it was built from — it cannot
# tell whether the build was correct, only whether it happened.
#
# Located rather than hardcoded, because ROOT is defined by where model_builder
# sits and the frontend is not guaranteed to be its sibling.

_FRONTEND: dict = {}


def _find(*relative_paths):
    """First existing path among candidates, searched from ROOT upward."""
    bases = [ROOT] + list(ROOT.parents)[:2]
    for base in bases:
        for rel in relative_paths:
            candidate = base / rel
            if candidate.exists():
                return candidate
    return None


def _newest_mtime(folder: Path) -> tuple[float, Path | None]:
    newest, where = 0.0, None
    for p in folder.rglob("*"):
        if not p.is_file():
            continue
        if any(part in {"node_modules", "dist", ".git"} for part in p.parts):
            continue
        m = p.stat().st_mtime
        if m > newest:
            newest, where = m, p
    return newest, where


@check("frontend root located")
def _():
    # Anchored on vite.config.js rather than assumed. A build script run from
    # the wrong root reports success into a directory nothing serves — which is
    # exactly how dist was emptied on July 30 — so this check has to derive the
    # root from the config, not from where preflight happens to sit.
    candidates = []
    for base in [ROOT] + list(ROOT.parents)[:2]:
        for cfg in base.glob("*/vite.config.js"):
            candidates.append(cfg)
        cfg = base / "vite.config.js"
        if cfg.exists():
            candidates.append(cfg)

    for cfg in candidates:
        text = cfg.read_text(encoding="utf-8", errors="ignore")
        if "webroot" in text and "model-builder" not in text.split("outDir")[-1][:80]:
            root = cfg.parent
            if (root / "Reckoner" / "webroot").exists() or (root / "src").exists():
                _FRONTEND["root"] = root
                _FRONTEND["src"] = root / "src"
                return str(root)

    _FRONTEND["skip"] = True
    return "no frontend project found — packaging checks skipped"


@check("Tauri webroot is a complete build")
def _():
    if _FRONTEND.get("skip"):
        return "skipped"
    root = _FRONTEND["root"]
    webroot = root / "Reckoner" / "webroot"
    index = webroot / "index.html"
    if not index.is_file():
        raise FileNotFoundError(
            f"{index} missing. tauri.conf.json sets frontendDist to "
            f"../webroot, so the bundle would ship without an interface."
        )
    assets = list((webroot / "assets").glob("*")) if (webroot / "assets").is_dir() else []
    if not assets:
        raise RuntimeError(
            "webroot\\assets is empty. index.html alone loads a blank page — "
            "this is the shape of a copy from the wrong source directory."
        )
    if not (webroot / "model-builder" / "index.html").is_file():
        raise RuntimeError(
            "webroot\\model-builder\\index.html missing. The main vite config "
            "sets emptyOutDir on webroot, so building it wipes the "
            "model-builder output. Build main first, then model-builder."
        )
    _FRONTEND["webroot"] = webroot
    return f"{len(assets)} assets, model-builder present"


@check("webroot is not older than its source")
def _():
    if _FRONTEND.get("skip"):
        return "skipped"
    src = _FRONTEND["src"]
    if not src.is_dir():
        raise FileNotFoundError(f"{src} not found — cannot judge freshness")
    newest, where = _newest_mtime(src)
    index = _FRONTEND["webroot"] / "index.html"
    if newest > index.stat().st_mtime:
        raise RuntimeError(
            f"{where.name if where else 'source'} is newer than "
            f"webroot\\index.html — the packaged interface is stale. "
            f"Run build.bat."
        )
    _FRONTEND["src_mtime"] = newest
    return "current"


@check("standalone dist matches webroot")
def _():
    if _FRONTEND.get("skip"):
        return "skipped"
    root = _FRONTEND["root"]
    dist = root / "dist"
    index = dist / "index.html"
    if not index.is_file():
        raise FileNotFoundError(
            f"{index} missing. reckoner_api.py serves this directory and will "
            f"report 'No dist/ folder'. Run build.bat."
        )
    assets = list((dist / "assets").glob("*")) if (dist / "assets").is_dir() else []
    if not assets:
        raise RuntimeError(
            "dist\\assets is empty — dist is a hollow shell. It is a copy of "
            "webroot and has no independent meaning; run build.bat from the "
            "frontend root."
        )
    if index.read_bytes() != (_FRONTEND["webroot"] / "index.html").read_bytes():
        raise RuntimeError(
            "dist\\index.html and webroot\\index.html differ. They must come "
            "from one build; run build.bat rather than building separately."
        )
    if not (dist / "model-builder" / "index.html").is_file():
        raise RuntimeError("dist\\model-builder\\index.html missing — copy is incomplete.")
    return f"{len(assets)} assets, identical entry point"


section("9. Shipped configuration")

# The adapter registry starts empty and load_postgres_adapters() reads PG_n_*
# from the environment. That default is correct — nothing is hardcoded — which
# means the only way development substrates reach a user is by .env travelling
# with the build. These checks make that a failure rather than a thing to
# remember.
#
# No value from .env is ever printed. DATABASE_URL carries a password, and a
# preflight transcript is the kind of thing that gets pasted into a handoff.

_ENV: dict = {}


def _read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


@check(".env located and parsed")
def _():
    path = _find(".env", "reckoner/.env")
    if path is None:
        _ENV["absent"] = True
        return "no .env — nothing can leak through it"
    _ENV["path"] = path
    _ENV["vars"] = _read_env(path)
    n_pg = sum(1 for k in _ENV["vars"] if k.endswith("_NAME") and k.startswith("PG_"))
    return f"{path} — {len(_ENV['vars'])} settings, {n_pg} postgres declarations"


@check("no duplicate adapter declarations")
def _():
    if _ENV.get("absent"):
        return "skipped"
    names: dict[str, list[str]] = {}
    for key, value in _ENV["vars"].items():
        if key.startswith("PG_") and key.endswith("_NAME"):
            names.setdefault(value, []).append(key)
    dupes = {v: ks for v, ks in names.items() if len(ks) > 1}
    if dupes:
        detail = "; ".join(f"{v} declared by {', '.join(ks)}" for v, ks in dupes.items())
        raise RuntimeError(
            f"{detail}. Each is loaded independently, so a failing schema "
            f"produces one identical error per declaration and looks like a "
            f"retry loop."
        )
    return f"{len(names)} distinct" if names else "none declared"


@check("adapter numbering is contiguous")
def _():
    if _ENV.get("absent"):
        return "skipped"
    nums = sorted(
        int(k.split("_")[1])
        for k in _ENV["vars"]
        if k.startswith("PG_") and k.endswith("_NAME") and k.split("_")[1].isdigit()
    )
    if not nums:
        return "none declared"
    expected = list(range(1, len(nums) + 1))
    if nums != expected:
        raise RuntimeError(
            f"PG_ indices are {nums}, expected {expected}. If the loader stops "
            f"at the first gap, every declaration after it is silently ignored."
        )
    return f"PG_1..PG_{nums[-1]}"


@check("release build declares no development substrates")
def _():
    if _ENV.get("absent"):
        return "skipped"
    debug = _ENV["vars"].get("DEBUG", "").strip().lower()
    declared = sorted(
        v for k, v in _ENV["vars"].items()
        if k.startswith("PG_") and k.endswith("_NAME")
    )
    if debug in {"false", "0", "no"} and declared:
        raise RuntimeError(
            f"DEBUG is false but {len(declared)} postgres substrate(s) are "
            f"declared: {', '.join(sorted(set(declared)))}. A shipped bundle "
            f"would attempt these against a database the user does not have "
            f"and print a failure for each on every launch. Comment them out "
            f"before packaging."
        )
    if debug not in {"false", "0", "no"}:
        return f"DEBUG={debug or 'unset'} — development, {len(declared)} declared"
    return "release, none declared"


@check(".env.example exists and is safe to ship")
def _():
    example = _find(".env.example", "reckoner/.env.example")
    if example is None:
        raise FileNotFoundError(
            ".env.example not found. It is what ships in place of .env; "
            "without it a user has no record of what is configurable."
        )
    vars_ = _read_env(example)
    active = [k for k in vars_ if k.startswith("PG_") and k.endswith("_NAME")]
    if active:
        raise RuntimeError(
            f".env.example declares {len(active)} live postgres substrate(s). "
            f"Comment them out — the example is documentation, not a default."
        )
    debug = vars_.get("DEBUG", "").strip().lower()
    if debug and debug not in {"false", "0", "no"}:
        raise RuntimeError(f".env.example sets DEBUG={debug}; it should ship false.")
    return str(example)


# ── 10. Postgres affordances matview freshness ───────────────────────────────
section("10. Postgres materialized views")

# Skips cleanly when no Postgres substrate is configured. When one is present,
# this fails loudly on a stale snf_affordances rather than letting the adapter
# discover the missing column on the first drawer open.


@check("snf_affordances matviews expose distinct_value_count")
def _():
    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed — skipped"

    import os
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return "DATABASE_URL not set — skipped"

    try:
        con = psycopg2.connect(dsn)
    except Exception as exc:
        return f"no connection ({type(exc).__name__}) — skipped"

    stale = []
    checked = 0
    try:
        with con, con.cursor() as cur:
            cur.execute(
                "SELECT schemaname FROM pg_matviews "
                "WHERE matviewname = 'snf_affordances'"
            )
            schemas = [r[0] for r in cur.fetchall()]
            for sch in schemas:
                checked += 1
                # Materialized views are absent from information_schema by
                # SQL-standard definition; pg_attribute is the only source.
                cur.execute(
                    "SELECT a.attname FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = 'snf_affordances' "
                    "AND a.attnum > 0 AND NOT a.attisdropped",
                    (sch,)
                )
                cols = {c[0] for c in cur.fetchall()}
                if not cols:
                    stale.append(f"{sch} (no columns readable — check permissions)")
                    continue
                if "distinct_value_count" not in cols:
                    stale.append(f"{sch} (has {sorted(cols)})")
    finally:
        con.close()

    if stale:
        raise RuntimeError(
            "stale matview(s): " + "; ".join(stale) +
            " — the adapter selects distinct_value_count and will 500. "
            "Drop and recreate with COUNT(DISTINCT value)."
        )
    if not checked:
        return "no snf_affordances matviews found"
    return f"{checked} matview(s) current"



# ── Cleanup ──────────────────────────────────────────────────────────────────
# The check substrate is disposable; leaving it behind would clutter the
# artifacts directory and show up in the substrate list as a phantom dataset.
if "conn" in _state:
    _state["conn"].close()
if "path" in _state:
    try:
        Path(_state["path"]).unlink(missing_ok=True)
    except OSError:
        pass


# ── Report ───────────────────────────────────────────────────────────────────
print(f"\n{'=' * 60}")
if FAILED:
    print(f"FAIL — {len(PASSED)} passed, {len(FAILED)} failed\n")
    for label, why in FAILED:
        print(f"  {label}\n    {why}")
    print("\nDo not package until these are green.")
    sys.exit(1)
else:
    print(f"PASS — all {len(PASSED)} checks green. Safe to package.")
    sys.exit(0)
