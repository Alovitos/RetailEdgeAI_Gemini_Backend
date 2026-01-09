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
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # 1. mapping
        sales_val_col = get_best_column(df, ["Total Sales", "Τζίρος", "Value Sales"])
        price_with_vat_col = get_best_column(df, ["Sales Price", "Τιμή Λιανικής", "Retail Price"])
        vat_col = get_best_column(df, ["VAT", "ΦΠΑ", "Tax"])
        cost_net_col = get_best_column(df, ["Net Price", "Cost Price", "Τιμή Αγοράς"])
        
        # 2. Calculations (Row-level)
        raw_price = pd.to_numeric(df[price_with_vat_col], errors='coerce').fillna(0)
        vat_val = pd.to_numeric(df[vat_col], errors='coerce').fillna(0)
        purchase_net = pd.to_numeric(df[cost_net_col], errors='coerce').fillna(0)
        
        vat_factor = np.where(vat_val >= 1, 1 + (vat_val / 100), 1 + vat_val)
        df['clean_sales_price'] = raw_price / vat_factor
        df['gm_percent'] = ((df['clean_sales_price'] - purchase_net) / df['clean_sales_price']).replace([np.inf, -np.inf, np.nan], 0) * 100

        # 3. Metadata
        df['sales'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['category'] = df[get_best_column(df, ["Category", "Κατηγορία"]) or "General"].astype(str)
        df['product_name'] = df[get_best_column(df, ["Description", "Name"]) or df.columns[0]].astype(str)

        # 4. ABC
        def calculate_category_abc(group):
            group = group.sort_values('sales', ascending=False)
            total = group['sales'].sum()
            if total <= 0: return group.assign(abc_class='C')
            cum_pct = (group['sales'].cumsum() / total) * 100
            group['abc_class'] = pd.cut(cum_pct, bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
            return group

        df = df.groupby('category', group_keys=False).apply(calculate_category_abc)

        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "raw_data": df[['product_name', 'category', 'sales', 'clean_sales_price', 'abc_class', 'gm_percent']].to_dict(orient='records'),
            "status": "success"
        }

        # Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed", 
            "analysis_json": result
        }).eq("id", project_id).execute()
        
        return {"status": "success"}

    except Exception as e:
        print(f"Error: {str(e)}")
        if project_id:
            # Χρησιμοποιούμε μόνο στήλες που ξέρουμε ότι υπάρχουν σίγουρα
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
