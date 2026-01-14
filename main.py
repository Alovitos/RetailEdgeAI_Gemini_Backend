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

# Supabase Setup
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        # Φόρτωση αρχείου
        response = requests.get(file_url, timeout=60)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # --- DYNAMIC COLUMN MAPPING ---
        id_col = next((c for c in df.columns if any(x in c.upper() for x in ["ID", "CODE", "ΚΩΔ"])), df.columns[0])
        name_col = next((c for c in df.columns if any(x in c.upper() for x in ["DESC", "NAME", "ΠΕΡΙΓ"])), df.columns[1])
        sales_val_col = "Value Sales"
        unit_sales_col = "Unit Sales"
        segment_col = "Segment"
        cost_col = "Net_Price"
        price_no_vat_col = "Sales_Without_VAT"

        # Conversion to numeric
        for col in [sales_val_col, unit_sales_col, cost_col, price_no_vat_col]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Margin Calculation
        df['gm_percent'] = np.where(df[price_no_vat_col] > 0, 
                                   ((df[price_no_vat_col] - df[cost_col]) / df[price_no_vat_col]) * 100, 0)
        
        # ABC Analysis for Recommendation Logic
        df = df.sort_values(sales_val_col, ascending=False)
        total_sales_sum = float(df[sales_val_col].sum())
        df['cum_perc'] = (df[sales_val_col].cumsum() / total_sales_sum) * 100 if total_sales_sum > 0 else 0
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # DOI Simulation (Based on ABC for realism)
        df['calculated_doi'] = np.where(df['abc_class'] == 'A', np.random.randint(7, 20, size=len(df)), np.random.randint(25, 90, size=len(df)))

        # --- CATEGORY MACRO DATA (For Bubble Chart) ---
        cat_group = df.groupby(segment_col).agg({
            sales_val_col: 'sum',
            unit_sales_col: 'sum',
            'gm_percent': 'mean'
        }).reset_index()
        
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[segment_col]),
                "sales": int(r[sales_val_col]),
                "units": int(r[unit_sales_col]),
                "avg_margin": round(float(r['gm_percent']), 1)
            })

        # --- SKU RAW DATA (For Drill-down Table) ---
        raw_data = []
        for _, row in df.iterrows():
            margin = float(row['gm_percent'])
            doi = float(row['calculated_doi'])
            abc = str(row['abc_class'])
            
            # Recommendation Logic
            if margin > 20 and abc == 'A': rec = "Expand"
            elif doi > 65 or (margin < 10 and abc == 'C'): rec = "Under Review"
            elif abc == 'A' and margin < 15: rec = "Price Adjust"
            else: rec = "Maintain"

            raw_data.append({
                "sku": str(row[id_col]),         # key 1
                "sku_id": str(row[id_col]),      # key 2
                "description": str(row[name_col]),# key 1
                "product_name": str(row[name_col]),# key 2
                "category": str(row[segment_col]),
                "units": int(row[unit_sales_col]),
                "sales": int(row[sales_val_col]),
                "gm_percent": round(margin, 1),
                "doi": round(doi, 1),
                "recommendation": rec,
                "smart_tag": rec
            })

        # Final JSON Response
        result = {
            "total_sales": int(total_sales_sum),
            "total_units": int(df[unit_sales_col].sum()),
            "category_macro": category_macro,
            "raw_data": raw_data,
            "status": "success"
        }

        # Update Supabase
        supabase.table("projects").update({
            "analysis_json": result, 
            "analysis_status": "completed"
        }).eq("id", project_id).execute()

        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
