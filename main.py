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
        
        # Καθαρισμός ονομάτων στηλών (Fix για το 'Series' object error)
        df.columns = [str(c).strip() for c in df.columns]

        # --- 1. EXACT MAPPING (image_2504c7.png) ---
        id_col = "SKU_ID"
        desc_col = "SKU_Description"
        brand_col = "Brand"
        cat_col = "Category"
        val_sales_col = "Value Sales"
        unit_sales_col = "Unit Sales"
        price_no_vat_col = "Sales_Price_Without_VAT"
        net_price_col = "Net_Price"

        # Μετατροπή στηλών σε αριθμούς με ασφάλεια
        cols_to_fix = [val_sales_col, unit_sales_col, price_no_vat_col, net_price_col]
        for col in cols_to_fix:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').str.strip(), errors='coerce').fillna(0)

        # --- 2. CORE CALCULATIONS (NRM & ABC) ---
        # GM% Calculation
        df['gm_percent'] = np.where(df[price_no_vat_col] > 0, 
                                    ((df[price_no_vat_col] - df[net_price_col]) / df[price_no_vat_col]) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values(val_sales_col, ascending=False)
        total_sales_sum = df[val_sales_col].sum()
        df['cum_perc'] = (df[val_sales_col].cumsum() / (total_sales_sum + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # --- 3. DATA PACKAGING (Legacy & PRO Support) ---
        all_items = []
        for _, row in df.iterrows():
            # Elasticity Logic
            elasticity = -2.4 if row['abc_class'] == 'A' else -1.6
            if any(x in str(row[cat_col]).upper() for x in ["ICE", "SOFT", "BEER"]): elasticity -= 0.6

            item = {
                "sku_id": str(row[id_col]),
                "name": str(row[desc_col]),
                "description": str(row[desc_col]),
                "category": str(row[cat_col]),
                "brand": str(row[brand_col]),
                "revenue": float(row[val_sales_col]),
                "sales": float(row[val_sales_col]),
                "units": int(row[unit_sales_col]),
                "price": round(float(row[price_no_vat_col]), 2),
                "current_price": round(float(row[price_no_vat_col]), 2),
                "net_price": round(float(row[net_price_col]), 2),
                "cost_price": round(float(row[net_price_col]), 2),
                "gm_percent": round(float(row['gm_percent']), 1),
                "abc_class": str(row['abc_class']),
                "elasticity": elasticity,
                "doi": np.random.randint(15, 45) # Simulated DOI
            }
            all_items.append(item)

        # Category Macro για το Bubble Chart
        cat_group = df.groupby(cat_col).agg({val_sales_col: 'sum', unit_sales_col: 'sum', 'gm_percent': 'mean'}).reset_index()
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[cat_col]),
                "sales": int(r[val_sales_col]),
                "units": int(r[unit_sales_col]),
                "avg_margin": round(float(r['gm_percent']), 1)
            })

        # Τελικό JSON που τα περιέχει ΟΛΑ
        result = {
            "items": all_items,             # Για Home & Dashboard
            "raw_data": all_items,          # Για Executive RGM & Promo Planner
            "category_macro": category_macro, # Για το Bubble Chart
            "total_sales": int(total_sales_sum),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        # Επιστροφή σφάλματος στο Supabase για debugging
        error_msg = {"error": str(e)}
        if project_id:
            supabase.table("projects").update({"analysis_json": error_msg, "analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
