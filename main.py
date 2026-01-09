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

        # 1. Έξυπνο Mapping
        sales_val_col = get_best_column(df, ["Total Sales", "Τζίρος", "Value Sales"])
        price_with_vat_col = get_best_column(df, ["Sales Price", "Τιμή Λιανικής", "Retail Price"])
        vat_col = get_best_column(df, ["VAT", "ΦΠΑ", "Tax"])
        cost_net_col = get_best_column(df, ["Net Price", "Cost Price", "Τιμή Αγοράς"])
        
        cat_col = get_best_column(df, ["Category", "Κατηγορία"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός"])

        # 2. Μετατροπή σε αριθμούς
        raw_price = pd.to_numeric(df[price_with_vat_col], errors='coerce').fillna(0)
        vat_rate = pd.to_numeric(df[vat_col], errors='coerce').fillna(0)
        # Αν το ΦΠΑ είναι σε μορφή π.χ. 24, το κάνουμε 0.24. Αν είναι 0.24, το αφήνουμε.
        vat_factor = np.where(vat_rate >= 1, vat_rate / 100, vat_rate)
        
        # 3. Δυναμική Αποφορολόγηση ανά Row
        df['clean_sales_price'] = raw_price / (1 + vat_factor)
        
        # 4. Υπολογισμός GM%
        purchase_net = pd.to_numeric(df[cost_net_col], errors='coerce').fillna(0)
        df['gm_percent'] = ((df['clean_sales_price'] - purchase_net) / df['clean_sales_price']).replace([np.inf, -np.inf, np.nan], 0) * 100

        # 5. Λοιπά δεδομένα
        df['sales'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['category'] = df[cat_col].astype(str) if cat_col else "General"
        df['brand'] = df[brand_col].astype(str) if brand_col else "N/A"
        df['product_name'] = (df[code_col].astype(str) + " - " + df[desc_col].astype(str)) if code_col and desc_col else df[desc_col]

        # 6. ABC Analysis (Contextual)
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

        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "raw_data": df[['brand', 'category', 'product_name', 'sales', 'clean_sales_price', 'abc_class', 'gm_percent']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
