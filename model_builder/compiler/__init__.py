from .duckdb   import emit_duckdb
from .postgres import emit_postgres_views
from .srf      import emit_srf_records


def compile_artifact(df, spec):
    backend = spec.get("target", {}).get("backend", "duckdb")

    if backend == "duckdb":
        dim_rows, meta_rows, result = emit_duckdb(df, spec)
        srf_count, srf_warnings = emit_srf_records(dim_rows, meta_rows, spec)
        result["warnings"].extend(srf_warnings)
        result["srf_exported"] = srf_count
        return result

    elif backend in ("postgres-views", "postgres-import"):
        return emit_postgres_views(df, spec)

    else:
        raise ValueError(f"Unknown backend: {backend!r}. Expected 'duckdb' or 'postgres-views'.")