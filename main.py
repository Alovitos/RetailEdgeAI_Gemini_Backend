from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
import pandas as pd
import io
import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

app = FastAPI()

# ΠΡΟΣΘΗΚΗ: Επιτρέπουμε στο Lovable να επικοινωνεί με το Render
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class AnalysisRequest(BaseModel):
    project_id: str
    file_url: str

@app.get("/")
def read_root():
    return {"status": "RetailEdge Backend is running"}

@app.post("/analyze")
async def analyze_excel(request: AnalysisRequest):
    try:
        response = requests.get(request.file_url)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Could not download file")
        
        file_content = io.BytesIO(response.content)
        # Διαβάζουμε το Excel - Παίρνουμε το πρώτο sheet
        df = pd.read_excel(file_content)
        
        # Καθαρισμός στηλών
        df.columns = df.columns.str.strip()
        
        # Υπολογισμός βασικών KPIs για το Dashboard
        summary = {
            "total_rows": int(len(df)),
            "total_sales": float(df.select_dtypes(include=['number']).sum().iloc[0]) if not df.select_dtypes(include=['number']).empty else 0,
            "columns": df.columns.tolist()
        }
        
        # Ενημέρωση Supabase
        supabase.table("projects").update({
            "status": "completed",
            "analysis_json": summary
        }).eq("id", request.project_id).execute()
        
        return {"status": "success", "data": summary}

    except Exception as e:
        supabase.table("projects").update({"status": "failed"}).eq("id", request.project_id).execute()
        return {"status": "error", "message": str(e)}
