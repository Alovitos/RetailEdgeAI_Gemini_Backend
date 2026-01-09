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
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group", "Ομάδα", "Department"])
        
        # Εντοπισμός Κωδικού και Περιγραφής
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός", "Item No"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name", "Προϊόν"])

        # 2. Δημιουργία Full Product Name (Κωδικός - Περιγραφή)
        if code_col and desc_col:
            df['display_name'] = df[code_col].astype(str) + " - " + df[desc_col].astype(str)
        elif desc_col:
            df['display_name'] = df[desc_col]
        else:
            df['display_name'] = df[code_col] if code_col else "Unknown Product"

        # 3. Καθαρισμός αριθμητικών
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)

        # 4. Προετοιμασία Δεδομένων για Φίλτρα
        # Παίρνουμε μοναδικές τιμές και αφαιρούμε τα NaNs
        unique_brands = sorted([str(x) for x in df[brand_col].dropna().unique()]) if brand_col else []
        unique_cats = sorted([str(x) for x in df[cat_col].dropna().unique()]) if cat_col else []
        unique_products = sorted([str(x) for x in df['display_name'].dropna().unique()])

        # 5. Analytics
        brand_summary = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(10).to_dict() if brand_col else {}
        cat_summary = df.groupby(cat_col)[sales_col].sum().sort_values(ascending=False).to_dict() if cat_col else {}
        prod_summary = df.groupby('display_name')[sales_col].sum().sort_values(ascending=False).head(15).to_dict()

        result = {
            "total_sales": round(float(df[sales_col].sum()), 2),
            "filters": {
                "brands": unique_brands,
                "categories": unique_cats,
                "products": unique_products
            },
            "top_brands": [{"name": k, "value": v} for k, v in brand_summary.items()],
            "top_categories": [{"name": k, "value": v} for k, v in cat_summary.items()],
            "top_products": [{"name": k, "sales": v} for k, v in prod_summary.items()],
            "raw_data": df.to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
