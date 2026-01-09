import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key) if url and key else None

def find_column(df_cols, keywords):
    """Βρίσκει την καλύτερη στήλη με βάση λέξεις-κλειδιά"""
    for col in df_cols:
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

        # Mapping Στηλών
        brand_col = find_column(df.columns, ["Brand", "Μάρκα"])
        mfr_col = find_column(df.columns, ["Manufacturer", "Κατασκευαστής"])
        sales_col = find_column(df.columns, ["Sales", "Πωλήσεις", "Value"])
        margin_col = find_column(df.columns, ["Margin", "Περιθώριο", "Profit"])
        price_col = find_column(df.columns, ["Price", "Τιμή"])

        # Μετατροπή σε νούμερα
        if sales_col: df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        if margin_col: df[margin_col] = pd.to_numeric(df[margin_col], errors='coerce').fillna(0)

        # Ανάλυση Brands για το γράφημα
        top_brands = []
        if brand_col and sales_col:
            brand_data = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5)
            top_brands = [{"name": str(k), "value": float(v)} for k, v in brand_data.items()]

        # Υπολογισμός KPI
        total_sales = float(df[sales_col].sum()) if sales_col else 0
        avg_margin = float(df[margin_col].mean()) if margin_col else 0

        result = {
            "total_sales": round(total_sales, 2),
            "total_volume": len(df),
            "avg_margin": round(avg_margin, 2),
            "top_brands": top_brands,
            "detected_fields": {
                "brand": brand_col,
                "manufacturer": mfr_col,
                "sales": sales_col
            }
        }

        if supabase:
            supabase.table("projects").update({
                "analysis_status": "completed",
                "analysis_json": result
            }).eq("id", project_id).execute()

        return {"status": "success"}

    except Exception as e:
        if supabase:
            supabase.table("projects").update({
                "analysis_status": "failed",
                "analysis_json": {"error": str(e)}
            }).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
