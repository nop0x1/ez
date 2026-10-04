import math
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb

app = FastAPI()

con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET unsafe_disable_etag_checks = true;")


def clean_record(row: dict) -> dict:
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in row.items()
    }


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


@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content="May the force be with you.", status_code=200)


@app.get("/schema")
def get_schema():
    try:
        url    = "https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_0.parquet"
        result = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{url}') LIMIT 1").df().to_dict(orient="records")
        return {"fields": result}
    except Exception as e:
        return {"error": str(e)}


@app.get("/FetchData")
def fetch_data(
    Number: str = Query(None),
    Name:   str = Query(None),
    FName:  str = Query(None),
    Email:  str = Query(None),
    Alt:    str = Query(None),
):
    filters_main = []
    filters_alt  = []

    if Number:
        if not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
            return JSONResponse(
                status_code=400,
                content={
                    "status":    "rejected",
                    "message":   "Invalid Number. Must be 10-15 digits.",
                    "Developer": "@FBI"
                }
            )
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

    shards = [Number[-1]] if Number else [str(i) for i in range(10)]

    main_records = []
    alt_records  = []

    for shard in shards:
        primary_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_{shard}.parquet"
        alt_url     = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/alt_master_shard_{shard}.parquet"

        try:
            query = f"""
                SELECT *, 'Main' AS _record_type FROM read_parquet('{primary_url}') WHERE {where_main}
                UNION ALL
                SELECT *, 'Alt'  AS _record_type FROM read_parquet('{alt_url}')     WHERE {where_alt}
            """
            for row in con.execute(query).df().to_dict(orient="records"):
                rec_type = row.pop("_record_type")
                cleaned  = clean_record(row)
                (main_records if rec_type == "Main" else alt_records).append(cleaned)
        except Exception:
            continue

    if not main_records and not alt_records:
        return JSONResponse(
            status_code=404,
            content={
                "status":    "not_found",
                "Developer": "@FBI"
            }
        )

    return {
        "status": "success",
        "Data": {
            "Main_Records": main_records,
            "Alt_Records":  alt_records
        },
        "Developer": "@FBI"
    }
