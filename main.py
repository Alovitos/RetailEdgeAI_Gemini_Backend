import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_best_column(df, keywords):
    for col in df.columns:
        if any(key.lower() in str(col).lower() for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # 1. Mapping Στηλών
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος", "Value"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα", "Επωνυμία", "Manufacturer"])
        prod_col = get_best_column(df, ["Description", "Περιγραφή", "Product", "Προϊόν", "Name", "SKU"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group", "Ομάδα"])

        # 2. Προετοιμασία Δεδομένων
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        
        # 3. Εξαγωγή Φίλτρων (Unique Values)
        filters = {
            "brands": sorted(df[brand_col].dropna().unique().tolist()) if brand_col else [],
            "categories": sorted(df[cat_col].dropna().unique().tolist()) if cat_col else [],
            "products": sorted(df[prod_col].dropna().unique().tolist()) if prod_col else []
        }

        # 4. Αρχικά Analytics (Full View)
        brand_summary = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(10) if brand_col else pd.Series()
        prod_summary = df.groupby(prod_col)[sales_col].sum().sort_values(ascending=False).head(10) if prod_col else pd.Series()

        result = {
            "total_sales": round(float(df[sales_col].sum()), 2),
            "filters": filters,
            "top_brands": [{"name": str(k), "value": float(v)} for k, v in brand_summary.items()],
            "top_products": [{"name": str(k), "sales": float(v)} for k, v in prod_summary.items()],
            "raw_data": df.to_dict(orient='records'), # Στέλνουμε τα δεδομένα για να φιλτράρει το Lovable τοπικά
            "status": "success"
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error"}
