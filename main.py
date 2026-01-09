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

        # 1. Mapping
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος", "Value"])
        price_col = get_best_column(df, ["Price", "Τιμή", "Rate", "Unit Price"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα", "Επωνυμία"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός"])

        # 2. Cleaning
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['unit_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0)
        df['category'] = df[cat_col].astype(str).str.strip() if cat_col else "General"
        df['brand'] = df[brand_col].astype(str).str.strip() if brand_col else "N/A"
        df['product'] = (df[code_col].astype(str) + " - " + df[desc_col].astype(str)) if code_col and desc_col else df[desc_col]

        # 3. Export raw data for client-side context analysis
        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "raw_data": df[['brand', 'category', 'product', 'sales', 'unit_price']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
