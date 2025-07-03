from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

@app.get("/")
def read_root():
    return {"message": "Football analytics API is live!"}

@app.get("/db-check")
def db_check():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        conn.close()
        return {"db_status": "Connected", "result": result}
    except Exception as e:
        return {"db_status": "Error", "detail": str(e)}

@app.get("/health")
def health_check():
    return {"status": "ok"}