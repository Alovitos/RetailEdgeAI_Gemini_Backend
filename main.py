import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Αρχικοποίηση με έλεγχο
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key) if url and key else None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")
        print(f"DEBUG: Processing project {project_id}")

        # 1. Download αρχείου
        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        
        # 2. Υπολογισμός εκατομμυρίου (Max Sum)
        df.columns = [str(c).strip() for c in df.columns]
        numeric_sums = df.select_dtypes(include=['number']).sum()
        sales_col = "Value Sales" if "Value Sales" in numeric_sums else numeric_sums.idxmax()
        total_sales = float(numeric_sums[sales_col])

        analysis_result = {
            "total_sales": round(total_sales, 2),
            "total_volume": len(df),
            "status": "success"
        }

        # 3. Ενημέρωση Supabase
        if supabase:
            print(f"DEBUG: Updating Supabase for project {project_id}")
            supabase.table("projects").update({
                "analysis_status": "completed",
                "analysis_json": analysis_result
            }).eq("id", project_id).execute()
            print("DEBUG: Update successful")
        else:
            print("DEBUG: Supabase client NOT initialized - Check Environment Variables")

        return {"status": "success"}

    except Exception as e:
        print(f"DEBUG ERROR: {str(e)}")
        if supabase and project_id:
            supabase.table("projects").update({
                "analysis_status": "failed",
                "analysis_json": {"error": str(e)}
            }).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
