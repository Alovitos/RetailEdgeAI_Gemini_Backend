import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        # 1. Κατέβασμα αρχείου με headers για αποφυγή μπλοκαρίσματος
        response = requests.get(file_url, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"Failed to download file: Status {response.status_code}")
            
        file_content = io.BytesIO(response.content)
        
        # 2. Ανάγνωση Excel - Δοκιμή με engine openpyxl
        try:
            df = pd.read_excel(file_content, engine='openpyxl')
        except Exception:
            # Αν αποτύχει, δοκιμή χωρίς engine (για παλιότερα formats)
            file_content.seek(0)
            df = pd.read_excel(file_content)
        
        df.columns = [str(c).strip() for c in df.columns]

        # 3. Υπολογισμός πωλήσεων (Max-Sum Logic)
        numeric_cols = df.select_dtypes(include=['number']).columns
        if not numeric_cols.empty:
            sums = df[numeric_cols].sum()
            sales_col = "Value Sales" if "Value Sales" in sums else sums.idxmax()
            total_sales = float(sums[sales_col])
        else:
            total_sales = 0
            sales_col = "None found"

        # 4. Ενημέρωση Supabase
        result = {
            "total_sales": round(total_sales, 2),
            "total_volume": len(df),
            "detected_column": sales_col,
            "status": "success"
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()
        
        return {"status": "success"}

    except Exception as e:
        supabase.table("projects").update({
            "analysis_status": "failed",
            "analysis_json": {"error": str(e)}
        }).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
