from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
import os

app = FastAPI()

# Load DATABASE_URL from AWS SSM Parameter Store
ssm = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-west-1"))
param = ssm.get_parameter(Name="/football/DATABASE_URL", WithDecryption=True)
DATABASE_URL = param["Parameter"]["Value"]

@app.get("/api")
def read_root():
    return {"message": "Football analytics API is live!"}

@app.get("/api/db-check")
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

@app.get("/api/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
