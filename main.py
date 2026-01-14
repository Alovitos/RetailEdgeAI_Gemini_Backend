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

        # --- 1. HARDCODED MAPPING (Based on your exact Excel titles) ---
        id_col = "SKU_ID"
        name_col = "SKU_Description"
        brand_col = "Brand"
        cat_col = "Category"
        sales_val_col = "Value Sales"
        units_col = "Unit Sales"
        price_no_vat_col = "Sales_Price_Without_VAT"
        net_price_col = "Net_Price"

        # Safe Numeric Conversion
        for col in [sales_val_col, units_col, price_no_vat_col, net_price_col]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').strip(), errors='coerce').fillna(0)

        # --- 2. CORE CALCULATIONS (NRM Logic) ---
        # GM% = (Price - Cost) / Price
        df['gm_percent'] = np.where(df[price_no_vat_col] > 0, 
                                    ((df[price_no_vat_col] - df[net_price_col]) / df[price_no_vat_col]) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values(sales_val_col, ascending=False)
        total_sales_sum = df[sales_val_col].sum()
        df['cum_perc'] = (df[sales_val_col].cumsum() / (total_sales_sum + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # DOI Simulation for Supply Chain View
        df['calculated_doi'] = np.where(df['abc_class'] == 'A', np.random.randint(8, 25, size=len(df)), np.random.randint(30, 95, size=len(df)))

        # --- 3. PRO FEATURES: CATEGORY MACRO (For Bubble Chart) ---
        cat_group = df.groupby(cat_col).agg({
            sales_val_col: 'sum',
            units_col: 'sum',
            'gm_percent': 'mean'
        }).reset_index()
        
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[cat_col]),
                "sales": int(r[sales_val_col]),
                "units": int(r[units_col]),
                "avg_margin": round(float(r['gm_percent']), 1)
            })

        # --- 4. PRO FEATURES: RAW DATA (For Table, Promo & Negotiation) ---
        raw_data = []
        for _, row in df.iterrows():
            abc_val = str(row['abc_class'])
            category_name = str(row[cat_col])
            margin = float(row['gm_percent'])
            
            # Recommendation Logic
            if margin > 22 and abc_val == 'A': rec = "Expand"
            elif margin < 10 or abc_val == 'C': rec = "Under Review"
            else: rec = "Maintain"

            # Elasticity Logic for Promo Planner
            elasticity = -2.4 if abc_val == 'A' else -1.6
            if any(x in category_name.upper() for x in ["ICE", "SOFT", "BEER", "SNACK"]):
                elasticity -= 0.7
            
            raw_data.append({
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "name": str(row[name_col]), # Legacy Support
                "category": category_name,
                "brand": str(row[brand_col]),
                "units": int(row[units_col]),
                "sales": float(row[sales_val_col]),
                "revenue": float(row[sales_val_col]), # Legacy Support
                "gm_percent": round(margin, 1),
                "abc_class": abc_val,
                "doi": round(float(row['calculated_doi']), 1),
                "recommendation": rec,
                "smart_tag": rec,
                "current_price": round(float(row[price_no_vat_col]), 2),
                "price": round(float(row[price_no_vat_col]), 2), # Legacy Support
                "cost_price": round(float(row[net_price_col]), 2),
                "net_price": round(float(row[net_price_col]), 2), # Support for table
                "elasticity": elasticity
            })

        # --- 5. FINAL UNIFIED JSON ---
        result = {
            "items": raw_data,              # Legacy Dashboards
            "category_macro": category_macro, # NRM Bubble Chart
            "raw_data": raw_data,           # Executive Table & Promo Planner
            "total_sales": int(total_sales_sum),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
