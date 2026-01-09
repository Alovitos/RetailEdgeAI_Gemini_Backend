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
        prod_col = get_best_column(df, ["Description", "Περιγραφή", "Product", "Προϊόν", "Name"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group", "Ομάδα"])

        # 2. Υπολογισμοί
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        total_sales = float(df[sales_col].sum())

        # Top 10 Products
        top_products = []
        if prod_col and sales_col:
            prod_summary = df.groupby(prod_col)[sales_col].sum().sort_values(ascending=False).head(10)
            top_products = [{"name": str(k), "sales": float(v)} for k, v in prod_summary.items()]

        # Top Brands
        top_brands = []
        if brand_col and sales_col:
            brand_summary = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5)
            top_brands = [{"name": str(k), "value": float(v)} for k, v in brand_summary.items()]

        # Category Analysis
        categories = []
        if cat_col and sales_col:
            cat_summary = df.groupby(cat_col)[sales_col].sum().sort_values(ascending=False)
            categories = [{"name": str(k), "value": float(v)} for k, v in cat_summary.items()]

        result = {
            "total_sales": round(total_sales, 2),
            "total_items": len(df),
            "top_products": top_products,
            "top_brands": top_brands,
            "categories": categories,
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
