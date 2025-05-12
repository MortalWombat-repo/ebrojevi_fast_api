from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import duckdb

app = FastAPI()

DB_PATH = "db/enumbers_db.duckdb"

@app.get("/database")
def get_database():
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            result = con.execute("SELECT * FROM enumbers").fetchall()
            columns = [desc[0] for desc in con.description]
            database = [dict(zip(columns, row)) for row in result]

            if not database:
                raise HTTPException(status_code=404, detail="Database is empty")

            return database
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/database/{codes:path}")
def get_items_by_codes(codes: str):
    try:
        code_list = [code.strip().upper() for code in codes.split("/") if code.strip()]
        with duckdb.connect(DB_PATH, read_only=True) as con:
            placeholders = ','.join(['?'] * len(code_list))
            query = f"SELECT * FROM enumbers WHERE upper(code) IN ({placeholders})"
            result = con.execute(query, tuple(code_list)).fetchall()
            columns = [desc[0] for desc in con.description]
            items = [dict(zip(columns, row)) for row in result]

            if not items:
                raise HTTPException(status_code=404, detail="Items not found")

            return items
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
