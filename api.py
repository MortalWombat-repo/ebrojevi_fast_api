from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import duckdb

app = FastAPI()

DB_PATH = "db/enumbers.duckdb"

@app.get("/database")
def get_database():
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            result = con.execute("SELECT * FROM enumbers").fetchall()
            columns = [desc[0] for desc in con.description]
            database = [dict(zip(columns, row)) for row in result]
            return {"database": database}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/database/{code}")
def get_item_by_code(code: str):
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            query = "SELECT * FROM enumbers WHERE code = ?"
            result = con.execute(query, (code,)).fetchall()

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            columns = [desc[0] for desc in con.description]
            item = dict(zip(columns, result[0]))
            return item
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
