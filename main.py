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
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")
        
        print(f"Starting analysis for project: {project_id}")

        # 1. Λήψη αρχείου με timeout και redirects
        response = requests.get(file_url, allow_redirects=True, timeout=30)
        if response.status_code != 200:
            raise Exception(f"Download failed: {response.status_code}")

        # 2. Φόρτωση στη μνήμη
        file_content = io.BytesIO(response.content)
        
        # 3. Δοκιμή ανάγνωσης με πολλαπλά engines
        try:
            df = pd.read_excel(file_content, engine='openpyxl')
        except:
            file_content.seek(0)
            df = pd.read_excel(file_content) # Default engine backup

        # 4. Καθαρισμός και Υπολογισμοί
        df.columns = [str(c).strip() for c in df.columns]
        numeric_df = df.select_dtypes(include=['number'])
        
        if not numeric_df.empty:
            sums = numeric_df.sum()
            sales_col = "Value Sales" if "Value Sales" in sums else sums.idxmax()
            total_sales = float(sums[sales_col])
        else:
            total_sales = 0
            sales_col = "None"

        result = {
            "total_sales": round(total_sales, 2),
            "total_volume": len(df),
            "detected_column": sales_col,
            "status": "success"
        }

        # 5. Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()
        
        print("Analysis completed successfully")
        return {"status": "success"}

    except Exception as e:
        error_str = str(e)
        print(f"Error: {error_str}")
        if project_id:
            supabase.table("projects").update({
                "analysis_status": "failed",
                "analysis_json": {"error": error_str}
            }).eq("id", project_id).execute()
        return {"status": "error", "message": error_str}
