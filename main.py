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

        # --- 1. DYNAMIC MAPPING ---
        id_col = next((c for c in df.columns if any(x in c.upper() for x in ["ID", "CODE", "ΚΩΔ"])), df.columns[0])
        name_col = next((c for c in df.columns if any(x in c.upper() for x in ["DESC", "NAME", "ΠΕΡΙΓ"])), df.columns[1])
        brand_col = next((c for c in df.columns if "BRAND" in c.upper() or "ΜΑΡΚΑ" in c.upper()), "Brand")
        cat_col = next((c for c in df.columns if any(x in c.upper() for x in ["SEGMENT", "CATEGORY", "ΚΑΤΗΓ"])), "Segment")
        
        # Προσθήκη στήλης Brand αν λείπει για να μην κρασάρει
        if brand_col not in df.columns:
            df[brand_col] = "Generic"

        # --- 2. CALCULATIONS ---
        df['sales'] = pd.to_numeric(df["Value Sales"], errors='coerce').fillna(0)
        df['units'] = pd.to_numeric(df["Unit Sales"], errors='coerce').fillna(0)
        df['cost'] = pd.to_numeric(df["Net_Price"], errors='coerce').fillna(0)
        df['price'] = pd.to_numeric(df["Sales_Without_VAT"], errors='coerce').fillna(0)
        df['margin_euro'] = df['price'] - df['cost']
        df['gm_percent'] = np.where(df['price'] > 0, (df['margin_euro'] / df['price']) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values('sales', ascending=False)
        total_sales_sum = df['sales'].sum()
        df['cum_perc'] = (df['sales'].cumsum() / (total_sales_sum + 0.01)) * 100
        df['abc'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # DOI Simulation for Executive Suite
        df['calculated_doi'] = np.where(df['abc'] == 'A', np.random.randint(8, 25, size=len(df)), np.random.randint(30, 95, size=len(df)))

        # --- 3. CATEGORY MACRO (For Level 1 - Bubble Chart) ---
        cat_group = df.groupby(cat_col).agg({
            'sales': 'sum',
            'units': 'sum',
            'gm_percent': 'mean'
        }).reset_index()
        
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[cat_col]),
                "sales": int(r['sales']),
                "units": int(r['units']),
                "avg_margin": round(float(r['gm_percent']), 1)
            })

        # --- 4. SKU & BRAND DATA (For Level 2 Table & Promo Planner) ---
        raw_data = []
        for _, row in df.iterrows():
            margin = float(row['gm_percent'])
            abc_val = str(row['abc'])
            category_name = str(row[cat_col])
            
            # Recommendation Logic
            if margin > 22 and abc_val == 'A': rec = "Expand"
            elif margin < 10 or abc_val == 'C': rec = "Under Review"
            else: rec = "Maintain"

            # Elasticity Logic for Promo Planner
            elasticity = -2.4 if abc_val == 'A' else -1.6
            if any(x in category_name.upper() for x in ["ICE", "SOFT", "BEER", "SNACK"]):
                elasticity -= 0.7 # Πιο ευαίσθητα προϊόντα
            
            raw_data.append({
                "sku": str(row[id_col]),
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "category": category_name,
                "brand": str(row[brand_col]),
                "units": int(row['units']),
                "sales": int(row['sales']),
                "gm_percent": round(margin, 1),
                "doi": round(float(row['calculated_doi']), 1),
                "recommendation": rec,
                "smart_tag": rec,
                "current_price": round(float(row['price']), 2),
                "cost_price": round(float(row['cost']), 2),
                "elasticity": elasticity,
                "abc_class": abc_val
            })

        # --- 5. FINAL RESPONSE ---
        result = {
            "total_sales": int(total_sales_sum),
            "category_macro": category_macro,
            "raw_data": raw_data,
            "status": "success"
        }

        # Update Supabase
        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
