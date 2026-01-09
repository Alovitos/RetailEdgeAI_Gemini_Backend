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

        # 1. Λήψη αρχείου
        response = requests.get(file_url)
        file_content = io.BytesIO(response.content)
        
        # 2. Ανάγνωση Excel με το σωστό engine
        df = pd.read_excel(file_content, engine='openpyxl')
        
        # 3. Καθαρισμός στηλών
        df.columns = [str(c).strip() for c in df.columns]

        # 4. Υπολογισμός αθροισμάτων για όλες τις στήλες
        numeric_sums = {}
        for col in df.columns:
            series = pd.to_numeric(df[col], errors='coerce')
            if series.notna().any():
                numeric_sums[col] = float(series.sum())

        # 5. Επιλογή στήλης Τζίρου (Προτίμηση στο 'Value Sales')
        if "Value Sales" in numeric_sums:
            sales_col = "Value Sales"
        else:
            sales_col = max(numeric_sums, key=numeric_sums.get) if numeric_sums else None
            
        total_sales = numeric_sums.get(sales_col, 0)
        total_volume = int(len(df)) # Ή άθροισμα στήλης 'Unit' αν υπάρχει

        # 6. Δημιουργία JSON αποτελέσματος
        summary = {
            "total_sales": round(total_sales, 2),
            "total_volume": total_volume,
            "detected_column": sales_col,
            "status": "success"
        }

        # 7. Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}

    except Exception as e:
        error_msg = str(e)
        supabase.table("projects").update({
            "analysis_status": "failed",
            "analysis_json": {"error": error_msg}
        }).eq("id", project_id).execute()
        return {"status": "error", "message": error_msg}
