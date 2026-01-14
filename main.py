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

        # --- MAPPING ---
        id_col, desc_col, brand_col, cat_col = "SKU_ID", "SKU_Description", "Brand", "Category"
        val_sales_col, unit_sales_col, price_col, net_price_col = "Value Sales", "Unit Sales", "Sales_Price_Without_VAT", "Net_Price"

        for col in [val_sales_col, unit_sales_col, price_col, net_price_col]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').strip(), errors='coerce').fillna(0)

        # --- CALCULATIONS ---
        df['gm_percent'] = np.where(df[price_col] > 0, ((df[price_col] - df[net_price_col]) / df[price_col]) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values(val_sales_col, ascending=False)
        total_sales = df[val_sales_col].sum()
        df['cum_perc'] = (df[val_sales_col].cumsum() / (total_sales + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # --- DATA PACKAGING ---
        all_items = []
        for _, row in df.iterrows():
            margin = float(row['gm_percent'])
            abc = str(row['abc_class'])
            cat = str(row[cat_col]).upper()
            
            # 1. SMART TAG LOGIC (Fixing image_26b47b)
            if margin > 30 and abc == 'A': tag = "Star Product"
            elif margin < 15 and abc == 'A': tag = "Volume Driver"
            elif margin > 30 and abc == 'C': tag = "Niche/Premium"
            elif margin < 10: tag = "Kill or Fix"
            else: tag = "Maintain"

            # 2. SMART ELASTICITY LOGIC (Fixing image_2661c3)
            # Default values based on typical retail categories
            elasticity = -1.8 # baseline
            if any(x in cat for x in ["SOFT", "BEER", "WATER", "BEV"]): elasticity = -2.5
            elif any(x in cat for x in ["DAIRY", "YOGURT", "MILK"]): elasticity = -2.0
            elif any(x in cat for x in ["PERSONAL", "SHAMPOO"]): elasticity = -1.4
            
            all_items.append({
                "sku_id": str(row[id_col]),
                "name": str(row[desc_col]),
                "description": str(row[desc_col]), # Ensure desc is here
                "category": str(row[cat_col]),
                "brand": str(row[brand_col]),
                "revenue": float(row[val_sales_col]),
                "sales": float(row[val_sales_col]),
                "units": int(row[unit_sales_col]),
                "price": round(float(row[price_col]), 2),
                "net_price": round(float(row[net_price_col]), 2),
                "gm_percent": round(margin, 1),
                "abc_class": abc,
                "smart_tag": tag,
                "elasticity": elasticity,
                "doi": np.random.randint(15, 45)
            })

        # Category Macro for Donut Chart (Fixing image_26612c)
        cat_group = df.groupby(cat_col).agg({val_sales_col: 'sum'}).reset_index()
        category_macro = []
        for _, r in cat_group.iterrows():
            perc = (r[val_sales_col] / total_sales) * 100
            category_macro.append({
                "category": str(r[cat_col]),
                "value": round(float(r[val_sales_col]), 2),
                "label": f"{r[cat_col]} ({perc:.1f}%)" # Added label with %
            })

        result = {"items": all_items, "raw_data": all_items, "category_macro": category_macro, "status": "success"}
        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
