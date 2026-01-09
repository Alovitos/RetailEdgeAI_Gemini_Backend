import pandas as pd
import io, os, requests
import numpy as np
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
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος"])
        price_col = get_best_column(df, ["Price", "Τιμή", "Retail Price", "Unit Price"])
        cost_col = get_best_column(df, ["Cost", "Κόστος", "Purchase Price"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός"])

        # 2. Data Processing
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['unit_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0)
        # Υπολογισμός Κόστους (αν λείπει, βάζουμε 70% της τιμής)
        df['unit_cost'] = pd.to_numeric(df[cost_col], errors='coerce').fillna(df['unit_price'] * 0.7) if cost_col else df['unit_price'] * 0.7
        
        # Margin Calculations
        df['gm_value'] = df['sales'] - (df['unit_cost'] * (df['sales'] / df['unit_price']).replace([np.inf, -np.inf], 0))
        df['gm_percent'] = (df['gm_value'] / df['sales']).replace([np.inf, -np.inf, np.nan], 0) * 100

        df['category'] = df[cat_col].astype(str) if cat_col else "General"
        df['brand'] = df[brand_col].astype(str) if brand_col else "N/A"
        df['product_name'] = (df[code_col].astype(str) + " - " + df[desc_col].astype(str)) if code_col and desc_col else df[desc_col]

        # 3. Contextual ABC Analysis
        def calculate_category_abc(group):
            group = group.sort_values('sales', ascending=False)
            total = group['sales'].sum()
            if total <= 0:
                group['abc_class'] = 'C'
                return group
            cum_pct = (group['sales'].cumsum() / total) * 100
            group['abc_class'] = pd.cut(cum_pct, bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
            return group

        df = df.groupby('category', group_keys=False).apply(calculate_category_abc)

        # 4. JSON Response (Standardized Keys)
        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "raw_data": df[['brand', 'category', 'product_name', 'sales', 'unit_price', 'abc_class', 'gm_percent']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
