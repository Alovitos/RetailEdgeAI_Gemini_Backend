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
        id_col = next((c for c in df.columns if any(x in c.upper() for x in ["ID", "CODE", "ΚΩΔ"])), df.columns[0])
        name_col = next((c for c in df.columns if any(x in c.upper() for x in ["DESC", "NAME", "ΠΕΡΙΓ"])), df.columns[1])
        brand_col = next((c for c in df.columns if "BRAND" in c.upper() or "ΜΑΡΚΑ" in c.upper()), "Brand")
        cat_col = next((c for c in df.columns if any(x in c.upper() for x in ["SEGMENT", "CATEGORY", "ΚΑΤΗΓ"])), "Segment")

        # Basic Conversions
        df['Value Sales'] = pd.to_numeric(df["Value Sales"], errors='coerce').fillna(0)
        df['Unit Sales'] = pd.to_numeric(df["Unit Sales"], errors='coerce').fillna(0)
        df['Net_Price'] = pd.to_numeric(df["Net_Price"], errors='coerce').fillna(0)
        df['Sales_Without_VAT'] = pd.to_numeric(df["Sales_Without_VAT"], errors='coerce').fillna(0)
        
        # ABC Analysis (Απαραίτητο για τα δωρεάν dashboards)
        df = df.sort_values('Value Sales', ascending=False)
        total_sales = df['Value Sales'].sum()
        df['cum_perc'] = (df['Value Sales'].cumsum() / (total_sales + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # --- DATA FOR FREE DASHBOARDS (Legacy Support) ---
        # Αυτά τα πεδία ψάχνει το "Home" και το "Dashboard"
        items_for_free = []
        for _, row in df.iterrows():
            items_for_free.append({
                "name": str(row[name_col]),
                "revenue": float(row['Value Sales']),
                "units": int(row['Unit Sales']),
                "abc_class": str(row['abc_class']),
                "price": float(row['Sales_Without_VAT'])
            })

        # --- DATA FOR EXECUTIVE RGM & PROMO PLANNER (PRO Features) ---
        category_macro = []
        cat_group = df.groupby(cat_col).agg({'Value Sales': 'sum', 'Unit Sales': 'sum', 'Sales_Without_VAT': 'mean', 'Net_Price': 'mean'}).reset_index()
        for _, r in cat_group.iterrows():
            margin_pct = ((r['Sales_Without_VAT'] - r['Net_Price']) / r['Sales_Without_VAT'] * 100) if r['Sales_Without_VAT'] > 0 else 0
            category_macro.append({
                "category": str(r[cat_col]),
                "sales": int(r['Value Sales']),
                "units": int(r['Unit Sales']),
                "avg_margin": round(float(margin_pct), 1)
            })

        raw_data_pro = []
        for _, row in df.iterrows():
            margin_pct = ((row['Sales_Without_VAT'] - row['Net_Price']) / row['Sales_Without_VAT'] * 100) if row['Sales_Without_VAT'] > 0 else 0
            # Elasticity Logic
            elasticity = -2.4 if row['abc_class'] == 'A' else -1.6
            
            raw_data_pro.append({
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "category": str(row[cat_col]),
                "brand": str(row[brand_col]) if brand_col in df.columns else "Generic",
                "units": int(row['Unit Sales']),
                "sales": float(row['Value Sales']),
                "gm_percent": round(float(margin_pct), 1),
                "current_price": round(float(row['Sales_Without_VAT']), 2),
                "cost_price": round(float(row['Net_Price']), 2),
                "elasticity": elasticity,
                "abc_class": str(row['abc_class']),
                "doi": np.random.randint(10, 60) # Simulated DOI
            })

        # --- THE COMBINED RESULT ---
        result = {
            "items": items_for_free,        # Για τα δωρεάν charts
            "category_macro": category_macro, # Για το PRO Bubble Chart
            "raw_data": raw_data_pro,       # Για το PRO Table & Planner
            "total_sales": int(total_sales),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
