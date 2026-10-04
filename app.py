from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb

app = FastAPI()

con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET unsafe_disable_etag_checks = true;")

@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content="May the force be with you.", status_code=200)

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@FBI"
            }
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "@FBI"}
    )

@app.get("/", response_class=HTMLResponse)
def root_landing_page():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "Invalid parameter. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@FBI"
            }
        )
    
    last_digit = Number[-1]
    
    primary_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_{last_digit}.parquet"
    alt_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/alt_master_shard_{last_digit}.parquet"
    
    try:
        query = f"""
            SELECT *, 'Main' AS _record_type FROM read_parquet('{primary_url}') WHERE mobile = '{Number}'
            UNION ALL
            SELECT *, 'Alt' AS _record_type FROM read_parquet('{alt_url}') WHERE alt = '{Number}'
        """
        
        raw_results = con.execute(query).df().to_dict(orient="records")
        
        main_records = []
        alt_records = []
        
        for row in raw_results:
            rec_type = row.pop('_record_type')
            if rec_type == 'Main':
                main_records.append(row)
            else:
                alt_records.append(row)
        
        if not main_records and not alt_records:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found", 
                    "phone": Number,
                    "Developer": "@FBI"
                }
            )
            
        return {
            "status": "success", 
            "Data": {
                "Main_Records": main_records,
                "Alt_Records": alt_records
            },
            "Developer": "@FBI"
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Database processing error: {str(e)}",
                "Developer": "@FBI"
            }
        )
