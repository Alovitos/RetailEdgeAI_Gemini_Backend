import pandas as pd
import io
import os
import requests
import numpy as np
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

supabase: Client = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY")
)

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=60)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        
        # Καθαρισμός κενών στα ονόματα των στηλών
        df.columns = [str(c).strip() for c in df.columns]

        # --- ΕΞΥΠΝΟ MAPPING ΜΕ FALLBACKS ---
        # Product Name
        name_col = next((c for c in ["SKU_De", "SKU_Description", "Description"] if c in df.columns), df.columns[0])
        
        # Sales
        sales_col = next((c for c in ["Value Sales", "Total Sales", "Sales"] if c in df.columns), None)
        
        # Net Price (Κόστος)
        net_cost_col = next((c for c in ["Net_Price", "Cost", "Net Price"] if c in df.columns), None)
        
        # Sales Price (Λιανική)
        retail_col = next((c for c in ["Sales_Without_V", "Sales_Price_With_V", "Retail Price"] if c in df.columns), None)

        # Brand & Category
        brand_col = next((c for c in ["Brand", "Μάρκα"] if c in df.columns), None)
        cat_col = next((c for c in ["Segment", "Category", "Κατηγορία"] if c in df.columns), None)

        # Μετατροπή σε νούμερα
        df['sales_val'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0) if sales_col else 0
        df['net_ret'] = pd.to_numeric(df[retail_col], errors='coerce').fillna(0) if retail_col else 0
        df['net_cost'] = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0) if net_cost_col else 0
        
        # GM% Calculation
        df['gm_percent'] = 0
        mask = df['net_ret'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'net_ret'] - df.loc[mask, 'net_cost']) / df.loc[mask, 'net_ret']) * 100

        # ABC Analysis
        df = df.sort_values('sales_val', ascending=False)
        total_sales = float(df['sales_val'].sum())
        
        if total_sales > 0:
            df['cum_perc'] = (df['sales_val'].cumsum() / total_sales) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        raw_data = []
        for _, row in df.iterrows():
            # Αφαίρεση ειδικών χαρακτήρων από το όνομα για το JSON
            clean_name = str(row[name_col]).encode('ascii', 'ignore').decode('ascii')
            raw_data.append({
                "product_name": clean_name,
                "category": str(row[cat_col]) if cat_col else "General",
                "brand": str(row[brand_col]) if brand_col else "N/A",
                "sales": round(float(row['sales_val']), 2),
                "clean_sales_price": round(float(row['net_ret']), 2),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": str(row['abc_class'])
            })

        result = {"total_sales": round(total_sales, 2), "raw_data": raw_data, "status": "success"}

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
