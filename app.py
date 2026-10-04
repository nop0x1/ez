import math
import hashlib
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor
import asyncio
import duckdb

app = FastAPI()

# ── DUCKDB ────────────────────────────────────────────────────────────────────
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET unsafe_disable_etag_checks = true;")
con.execute("SET threads = 8;")
con.execute("SET http_keep_alive = true;")
con.execute("SET http_retries = 3;")
con.execute("SET enable_http_metadata_cache = true;")
con.execute("SET memory_limit = '512MB';")

# ── CACHE ─────────────────────────────────────────────────────────────────────
cache    = TTLCache(maxsize=500, ttl=3600)  # 500 results, 1 hour expiry
executor = ThreadPoolExecutor(max_workers=4)

BASE_URL = "https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve"


# ── VIEWS ─────────────────────────────────────────────────────────────────────
@app.on_event("startup")
def build_views():
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW main_data AS
            SELECT * FROM read_parquet('{BASE_URL}/final_master_shard_*.parquet')
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW alt_data AS
            SELECT * FROM read_parquet('{BASE_URL}/alt_master_shard_*.parquet')
        """)
        print("Views ready.")
    except Exception as e:
        print(f"View build failed: {e}")


# ── HELPERS ───────────────────────────────────────────────────────────────────
def clean_record(row: dict) -> dict:
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in row.items()
    }

def get_cache_key(*args) -> str:
    return hashlib.md5(str(args).encode()).hexdigest()

def run_query(where_main: str, where_alt: str, shard: str = None, limit: int = 50) -> dict:
    main_records = []
    alt_records  = []

    if shard is not None:
        # ── single shard — fast path ─────────────────────────────────────────
        primary_url = f"{BASE_URL}/final_master_shard_{shard}.parquet"
        alt_url     = f"{BASE_URL}/alt_master_shard_{shard}.parquet"
        query = f"""
            SELECT *, 'Main' AS _record_type
            FROM read_parquet('{primary_url}')
            WHERE {where_main}
            UNION ALL
            SELECT *, 'Alt' AS _record_type
            FROM read_parquet('{alt_url}')
            WHERE {where_alt}
            LIMIT {limit}
        """
        try:
            for row in con.execute(query).df().to_dict(orient="records"):
                rec_type = row.pop("_record_type")
                cleaned  = clean_record(row)
                (main_records if rec_type == "Main" else alt_records).append(cleaned)
        except Exception:
            pass
    else:
        # ── all shards via view ──────────────────────────────────────────────
        query = f"""
            SELECT *, 'Main' AS _record_type
            FROM main_data
            WHERE {where_main}
            UNION ALL
            SELECT *, 'Alt' AS _record_type
            FROM alt_data
            WHERE {where_alt}
            LIMIT {limit}
        """
        try:
            for row in con.execute(query).df().to_dict(orient="records"):
                rec_type = row.pop("_record_type")
                cleaned  = clean_record(row)
                (main_records if rec_type == "Main" else alt_records).append(cleaned)
        except Exception:
            pass

    return {"Main_Records": main_records, "Alt_Records": alt_records}


# ── EXCEPTION HANDLER ─────────────────────────────────────────────────────────
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status":    "rejected",
                "message":   "Invalid endpoint.",
                "Developer": "@FBI"
            }
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "@FBI"}
    )


# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content="May the force be with you.", status_code=200)


@app.get("/schema")
def get_schema():
    try:
        url    = f"{BASE_URL}/final_master_shard_0.parquet"
        result = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{url}') LIMIT 1"
        ).df().to_dict(orient="records")
        return {"fields": result}
    except Exception as e:
        return {"error": str(e)}


@app.get("/FetchData")
async def fetch_data(
    Number: str = Query(None),
    Name:   str = Query(None),
    FName:  str = Query(None),
    Email:  str = Query(None),
    Alt:    str = Query(None),
    Limit:  int = Query(50),
):
    # ── validate ──────────────────────────────────────────────────────────────
    if Number and (not Number.isdigit() or len(Number) < 10 or len(Number) > 15):
        return JSONResponse(
            status_code=400,
            content={
                "status":    "rejected",
                "message":   "Invalid Number. Must be 10-15 digits.",
                "Developer": "@FBI"
            }
        )

    limit = min(Limit, 100)

    # ── build filters ─────────────────────────────────────────────────────────
    filters_main = []
    filters_alt  = []

    if Number:
        filters_main.append(f"mobile = '{Number}'")
        filters_alt.append(f"alt = '{Number}'")
    if Name:
        filters_main.append(f"name ILIKE '%{Name}%'")
        filters_alt.append(f"name ILIKE '%{Name}%'")
    if FName:
        filters_main.append(f"fname ILIKE '%{FName}%'")
        filters_alt.append(f"fname ILIKE '%{FName}%'")
    if Email:
        filters_main.append(f"email ILIKE '%{Email}%'")
        filters_alt.append(f"email ILIKE '%{Email}%'")
    if Alt:
        filters_main.append(f"alt = '{Alt}'")
        filters_alt.append(f"alt = '{Alt}'")

    if not filters_main:
        return JSONResponse(
            status_code=400,
            content={
                "status":    "rejected",
                "message":   "Provide at least one parameter: Number, Name, FName, Email, Alt",
                "Developer": "@FBI"
            }
        )

    where_main = " AND ".join(filters_main)
    where_alt  = " AND ".join(filters_alt)
    shard      = Number[-1] if Number else None

    # ── check cache ───────────────────────────────────────────────────────────
    cache_key = get_cache_key(Number, Name, FName, Email, Alt, limit)
    if cache_key in cache:
        return {
            "status":  "success",
            "cached":  True,
            "Data":    cache[cache_key],
            "Developer": "@FBI"
        }

    # ── run query in thread ───────────────────────────────────────────────────
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, run_query, where_main, where_alt, shard, limit
    )

    main_records = result["Main_Records"]
    alt_records  = result["Alt_Records"]

    if not main_records and not alt_records:
        return JSONResponse(
            status_code=404,
            content={
                "status":    "not_found",
                "Developer": "@FBI"
            }
        )

    data = {"Main_Records": main_records, "Alt_Records": alt_records}

    # ── store in cache ────────────────────────────────────────────────────────
    cache[cache_key] = data

    return {
        "status":    "success",
        "cached":    False,
        "Data":      data,
        "Developer": "@FBI"
}
