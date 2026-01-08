from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import pandas as pd
import io
import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id") or body.get("projectId")
        file_url = body.get("file_url") or body.get("fileUrl")

        if not project_id or not file_url:
            return {"status": "error", "message": "Missing IDs"}

        # Λήψη και ανάγνωση αρχείου
        response = requests.get(file_url)
        df = pd.read_excel(io.BytesIO(response.content))
        df.columns = df.columns.str.strip()
        
        # Υπολογισμός KPIs
        numeric_df = df.select_dtypes(include=['number'])
        total_sales = float(numeric_df.iloc[:, 0].sum()) if not numeric_df.empty else 0
        
        summary = {
            "total_rows": int(len(df)),
            "total_sales": round(total_sales, 2),
            "columns": df.columns.tolist()
        }

        # ΔΙΟΡΘΩΣΗ: Ενημερώνουμε το analysis_status (όχι το status)
        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}

    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
