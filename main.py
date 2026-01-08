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
        project_id, file_url = body.get("project_id"), body.get("file_url")

        response = requests.get(file_url)
        df = pd.read_excel(io.BytesIO(response.content))
        
        # Καθαρισμός στηλών: Μετατροπή όλων των πιθανών αριθμών σε numeric
        # και αφαίρεση κενών από τα ονόματα
        df.columns = [c.strip() for c in df.columns]
        
        sums = {}
        for col in df.columns:
            # Προσπαθούμε να μετατρέψουμε τη στήλη σε αριθμούς (αν δεν είναι ήδη)
            numeric_col = pd.to_numeric(df[col], errors='coerce')
            if numeric_col.notna().any():
                sums[col] = numeric_col.sum()
        
        # Η στήλη με το μεγαλύτερο άθροισμα είναι ο Τζίρος μας
        sales_col = max(sums, key=sums.get) if sums else None
        total_sales = sums[sales_col] if sales_col else 0
        
        # Η στήλη με το δεύτερο μεγαλύτερο (συνήθως) ή που περιέχει 'Volume/Weekly' είναι οι μονάδες
        # Εδώ για σιγουριά παίρνουμε το 'Value Sales' αν υπάρχει ως όνομα, αλλιώς το Max
        if "Value Sales" in df.columns:
            sales_col = "Value Sales"
            total_sales = pd.to_numeric(df[sales_col], errors='coerce').sum()

        summary = {
            "total_rows": int(len(df)),
            "total_sales": float(total_sales),
            "detected_column": sales_col,
            "all_sums": {str(k): float(v) for k, v in sums.items()} # Για debug
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
