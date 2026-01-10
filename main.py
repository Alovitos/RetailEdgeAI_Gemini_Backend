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

def get_smart_elasticity(category_name, product_name):
    text = (str(category_name) + " " + str(product_name)).lower()
    if any(x in text for x in ['baby', 'diaper', 'pampers', 'nappy', 'πάνες', 'βρεφικά']): return -0.6
    if any(x in text for x in ['ice cream', 'snack', 'chips', 'lays', 'παγωτό', 'τσιπς']): return -3.2
    if any(x in text for x in ['yogurt', 'cheese', 'milk', 'γιαούρτι', 'τυρί']): return -1.5
    return -1.8

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
        name_col = "SKU_Description"
        sales_val_col = "Value Sales"
        unit_sales_col = "Unit Sales"
        retail_no_vat = "Sales_Without_VAT"
        retail_with_vat = "Sales_Price_With_VAT"
        cost_net = "Net_Price"
        segment_col = "Segment"
        brand_col = "Brand"
        
        # Προαιρετικές στήλες Stock/DOI
        stock_col = "Stock" if "Stock" in df.columns else None
        doi_col = "DOI" if "DOI" in df.columns else None

        # Μετατροπές & Βασικοί Υπολογισμοί
        df['sales_total'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['units_total'] = pd.to_numeric(df[unit_sales_col], errors='coerce').fillna(0)
        df['price_no_vat'] = pd.to_numeric(df[retail_no_vat], errors='coerce').fillna(0)
        df['price_vat'] = pd.to_numeric(df[retail_with_vat], errors='coerce').fillna(0)
        df['cost_net'] = pd.to_numeric(df[cost_net], errors='coerce').fillna(0)
        df['gm_percent'] = np.where(df['price_no_vat'] > 0, ((df['price_no_vat'] - df['cost_net']) / df['price_no_vat']) * 100, 0)
        
        # Υπολογισμός DOI (Days of Inventory)
        if stock_col:
            df['stock_val'] = pd.to_numeric(df[stock_col], errors='coerce').fillna(0)
            df['calculated_doi'] = np.where(df['units_total'] > 0, (df['stock_val'] / (df['units_total'] / 30)), 999)
        else:
            df['calculated_doi'] = pd.to_numeric(df[doi_col], errors='coerce').fillna(0) if doi_col else 0

        # ABC Analysis (Freemium)
        df = df.sort_values('sales_total', ascending=False)
        total_sales_sum = float(df['sales_total'].sum())
        if total_sales_sum > 0:
            df['cum_perc'] = (df['sales_total'].cumsum() / total_sales_sum) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        # --- ΕΠΙΠΕΔΟ 1: CATEGORY MACRO ANALYSIS (Premium) ---
        cat_analysis = df.groupby(segment_col).agg({
            'sales_total': 'sum',
            'units_total': 'sum',
            'gm_percent': 'mean'
        }).reset_index()
        cat_analysis['sales_share'] = (cat_analysis['sales_total'] / total_sales_sum) * 100
        cat_analysis_dict = cat_analysis.to_dict('records')

        # --- ΕΠΙΠΕΔΟ 2: SKU MICRO ANALYSIS & RAW DATA ---
        raw_data = []
        for _, row in df.iterrows():
            # Premium Recommendations Logic
            rec = "Maintain"
            if row['gm_percent'] > 25 and row['calculated_doi'] < 25 and row['abc_class'] == 'A':
                rec = "Focus & Expand"
            elif row['gm_percent'] < 12 and row['calculated_doi'] > 60:
                rec = "Delist Candidate"
            elif row['calculated_doi'] > 90:
                rec = "Liquidation Action"

            raw_data.append({
                "product_name": str(row[name_col]),
                "category": str(row[segment_col]),
                "brand": str(row[brand_col]) if brand_col in df.columns else "Generic",
                "units": int(row['units_total']),
                "sales": int(row['sales_total']),
                "clean_sales_price": round(float(row['price_vat']), 2),
                "net_price": round(float(row['cost_net']), 2),
                "gm_percent": round(float(row['gm_percent']), 1),
                "abc_class": str(row['abc_class']),
                "suggested_elasticity": float(get_smart_elasticity(str(row[segment_col]), str(row[name_col]))),
                "doi": round(float(row['calculated_doi']), 1),
                "recommendation": rec
            })

        result = {
            "total_sales": int(round(total_sales_sum, 0)),
            "total_units": int(df['units_total'].sum()),
            "category_macro": cat_analysis_dict,
            "raw_data": raw_data,
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
