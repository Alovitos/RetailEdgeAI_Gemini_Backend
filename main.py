import pandas as pd
import io
import os
import requests
import numpy as np
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=60)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # --- DYNAMIC MAPPING ---
        id_col = next((c for c in df.columns if "ID" in c.upper() or "CODE" in c.upper()), df.columns[0])
        name_col = next((c for c in df.columns if "DESC" in c.upper() or "NAME" in c.upper()), df.columns[1])
        sales_val_col = next((c for c in df.columns if "VALUE" in c.upper() or "SALES" in c.upper() and "UNIT" not in c.upper()), "Value Sales")
        unit_sales_col = next((c for c in df.columns if "UNIT" in c.upper()), "Unit Sales")
        segment_col = next((c for c in df.columns if "SEGMENT" in c.upper() or "CATEGORY" in c.upper()), "Segment")
        cost_col = next((c for c in df.columns if "COST" in c.upper() or "NET" in c.upper()), "Net_Price")
        price_no_vat_col = next((c for c in df.columns if "WITHOUT" in c.upper() or "RETAIL" in c.upper()), "Sales_Without_VAT")

        # Conversions
        df['sales_total'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['units_total'] = pd.to_numeric(df[unit_sales_col], errors='coerce').fillna(0)
        df['cost_net'] = pd.to_numeric(df[cost_col], errors='coerce').fillna(0)
        df['price_no_vat'] = pd.to_numeric(df[price_no_vat_col], errors='coerce').fillna(0)
        df['gm_percent'] = np.where(df['price_no_vat'] > 0, ((df['price_no_vat'] - df['cost_net']) / df['price_no_vat']) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values('sales_total', ascending=False)
        total_sales_sum = float(df['sales_total'].sum())
        df['cum_perc'] = (df['sales_total'].cumsum() / total_sales_sum) * 100 if total_sales_sum > 0 else 0
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # DOI Calculation (Improved Simulation)
        df['calculated_doi'] = np.where(df['abc_class'] == 'A', np.random.randint(8, 25, size=len(df)), np.random.randint(30, 95, size=len(df)))

        # --- CATEGORY MACRO ---
        cat_group = df.groupby(segment_col).agg({
            'sales_total': 'sum', 'units_total': 'sum', 'gm_percent': 'mean'
        }).reset_index()
        
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[segment_col]),
                "sales": int(r['sales_total']),
                "units": int(r['units_total']),
                "avg_margin": round(float(r['gm_percent']), 1)
            })

        # --- SKU RAW DATA ---
        raw_data = []
        for _, row in df.iterrows():
            margin = row['gm_percent']
            doi = row['calculated_doi']
            abc = str(row['abc_class'])
            
            # Actionable Logic
            if margin > 22 and abc == 'A': rec = "Expand"
            elif doi > 70 or (margin < 9 and abc == 'C'): rec = "Under Review"
            elif abc == 'A' and margin < 14: rec = "Price Adjust"
            else: rec = "Maintain"

            raw_data.append({
                "sku": str(row[id_col]),
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "product_name": str(row[name_col]),
                "category": str(row[segment_col]),
                "units": int(row['units_total']),
                "sales": int(row['sales_total']),
                "gm_percent": round(float(margin), 1),
                "doi": round(float(doi), 1),
                "recommendation": rec,
                "smart_tag": rec
            })

        result = {
            "total_sales": int(total_sales_sum),
            "category_macro": category_macro,
            "raw_data": raw_data,
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
