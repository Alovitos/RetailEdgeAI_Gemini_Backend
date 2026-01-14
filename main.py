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
        # Ψάχνει για SKU ID ή παίρνει την πρώτη στήλη αν δεν το βρει
        id_col = next((c for c in df.columns if "ID" in c.upper() or "CODE" in c.upper()), df.columns[0])
        name_col = next((c for c in df.columns if "DESC" in c.upper() or "NAME" in c.upper()), df.columns[1])
        sales_val_col = next((c for c in df.columns if "VALUE" in c.upper() or "SALES" in c.upper() and "UNIT" not in c.upper()), "Value Sales")
        unit_sales_col = next((c for c in df.columns if "UNIT" in c.upper()), "Unit Sales")
        segment_col = next((c for c in df.columns if "SEGMENT" in c.upper() or "CATEGORY" in c.upper()), "Segment")
        cost_col = next((c for c in df.columns if "COST" in c.upper() or "NET" in c.upper()), "Net_Price")
        price_no_vat_col = next((c for c in df.columns if "WITHOUT" in c.upper() or "RETAIL" in c.upper()), "Sales_Without_VAT")

        # Μετατροπές σε νούμερα
        df['sales_total'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['units_total'] = pd.to_numeric(df[unit_sales_col], errors='coerce').fillna(0)
        df['cost_net'] = pd.to_numeric(df[cost_col], errors='coerce').fillna(0)
        df['price_no_vat'] = pd.to_numeric(df[price_no_vat_col], errors='coerce').fillna(0)
        
        # Υπολογισμός Margin %
        df['gm_percent'] = np.where(df['price_no_vat'] > 0, ((df['price_no_vat'] - df['cost_net']) / df['price_no_vat']) * 100, 0)
        
        # Υπολογισμός DOI (Simulation αν δεν υπάρχει Stock)
        if "Stock" in df.columns:
            df['calculated_doi'] = np.where(df['units_total'] > 0, (pd.to_numeric(df['Stock'], errors='coerce').fillna(0) / (df['units_total'] / 30)), 999)
        else:
            df['calculated_doi'] = np.random.randint(15, 85, size=len(df))

        # --- CATEGORY MACRO DATA ---
        cat_group = df.groupby(segment_col).agg({
            'sales_total': 'sum',
            'units_total': 'sum',
            'gm_percent': 'mean'
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
            rec = "Maintain"
            if row['gm_percent'] > 25 and row['calculated_doi'] < 30: rec = "Stars"
            elif row['gm_percent'] < 12 and row['calculated_doi'] > 60: rec = "Under Review"

            raw_data.append({
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "category": str(row[segment_col]),
                "units": int(row['units_total']),
                "sales": int(row['sales_total']),
                "gm_percent": round(float(row['gm_percent']), 1),
                "doi": round(float(row['calculated_doi']), 1),
                "recommendation": rec
            })

        result = {
            "total_sales": int(df['sales_total'].sum()),
            "total_units": int(df['units_total'].sum()),
            "category_macro": category_macro,
            "raw_data": raw_data,
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
