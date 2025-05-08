from fastapi import FastAPI
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
