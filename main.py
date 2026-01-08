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
        df.columns = df.columns.str.strip().str.lower() # Μετατροπή σε πεζά για ευκολία

        # --- ΕΞΥΠΝΟΣ ΕΝΤΟΠΙΣΜΟΣ ΣΤΗΛΩΝ ---
        # Ψάχνουμε για στήλες που περιέχουν αυτές τις λέξεις
        sales_keywords = ['sales', 'πωλήσεις', 'amount', 'net revenue', 'τζίρος', 'value']
        volume_keywords = ['volume', 'ποσότητα', 'units', 'qty', 'τεμάχια', 'pieces']
        brand_keywords = ['brand', 'μάρκα', 'manufacturer', 'supplier']

        # Logic για Sales
        sales_col = next((c for c in df.columns if any(k in c for k in sales_keywords)), None)
        # Logic για Volume
        volume_col = next((c for c in df.columns if any(k in c for k in volume_keywords)), None)
        # Logic για Brands
        brand_col = next((c for c in df.columns if any(k in c for k in brand_keywords)), None)

        total_sales = float(df[sales_col].sum()) if sales_col else 0
        total_volume = float(df[volume_col].sum()) if volume_col else 0
        
        # Υπολογισμός Market Share αν υπάρχει Brand
        brand_data = {}
        if brand_col and sales_col:
            brand_data = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5).to_dict()

        summary = {
            "total_rows": int(len(df)),
            "total_sales": round(total_sales, 2),
            "total_volume": round(total_volume, 2),
            "detected_sales_column": sales_col,
            "brand_distribution": brand_data,
            "columns": df.columns.tolist()
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
