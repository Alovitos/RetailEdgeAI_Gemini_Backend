import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Έλεγχος αν υπάρχουν τα κλειδιά
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    print("CRITICAL ERROR: Supabase credentials missing in Render Environment Variables!")

supabase: Client = create_client(url, key) if url and key else None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")
        print(f"--- Processing Project: {project_id} ---")

        # 1. Κατέβασμα αρχείου
        print(f"Downloading from: {file_url}")
        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        print("File downloaded and read successfully.")

        # 2. Υπολογισμοί
        df.columns = [str(c).strip() for c in df.columns]
        numeric_sums = df.select_dtypes(include=['number']).sum()
        sales_col = "Value Sales" if "Value Sales" in numeric_sums else numeric_sums.idxmax()
        total_sales = float(numeric_sums[sales_col])
        
        result_data = {
            "total_sales": round(total_sales, 2),
            "total_volume": len(df),
            "status": "success"
        }
        print(f"Analysis Done: {result_data}")

        # 3. Ενημέρωση Supabase (Εδώ είναι το πιθανό κόλλημα)
        if supabase:
            print("Attempting to update Supabase...")
            update_response = supabase.table("projects").update({
                "analysis_status": "completed",
                "analysis_json": result_data
            }).eq("id", project_id).execute()
            print(f"Supabase Update Result: {update_response}")
        else:
            print("Supabase client not initialized!")

        return {"status": "success"}

    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: {error_msg}")
        if supabase and project_id:
            supabase.table("projects").update({
                "analysis_status": "failed",
                "analysis_json": {"error": error_msg}
            }).eq("id", project_id).execute()
        return {"status": "error", "message": error_msg}
