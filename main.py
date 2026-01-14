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
        # Αναζήτηση στηλών με βάση πιθανά ονόματα
        id_col = next((c for c in df.columns if any(x in c.upper() for x in ["ID", "CODE", "ΚΩΔ"])), "SKU_ID")
        name_col = next((c for c in df.columns if any(x in c.upper() for x in ["DESC", "NAME", "ΠΕΡΙΓ"])), "Description")
        brand_col = next((c for c in df.columns if "BRAND" in c.upper() or "ΜΑΡΚΑ" in c.upper()), "Brand")
        cat_col = next((c for c in df.columns if any(x in c.upper() for x in ["SEGMENT", "CATEGORY", "ΚΑΤΗΓ"])), "Segment")

        # Conversions & Calculations
        df['sales'] = pd.to_numeric(df["Value Sales"], errors='coerce').fillna(0)
        df['units'] = pd.to_numeric(df["Unit Sales"], errors='coerce').fillna(0)
        df['cost'] = pd.to_numeric(df["Net_Price"], errors='coerce').fillna(0)
        df['price'] = pd.to_numeric(df["Sales_Without_VAT"], errors='coerce').fillna(0)
        df['margin_pct'] = np.where(df['price'] > 0, ((df['price'] - df['cost']) / df['price']) * 100, 0)
        
        # ABC Analysis
        df = df.sort_values('sales', ascending=False)
        df['cum_perc'] = (df['sales'].cumsum() / (df['sales'].sum() + 0.01)) * 100
        df['abc'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # --- SKU RAW DATA FOR PROMO PLANNER ---
        raw_data = []
        for _, row in df.iterrows():
            # Logic: Τα A-Brands σε ευαίσθητες κατηγορίες έχουν μεγαλύτερη ελαστικότητα
            elasticity = -2.4 if row['abc'] == 'A' else -1.6
            if "ICE" in str(row[cat_col]).upper(): elasticity -= 0.5 # π.χ. Παγωτά
            
            raw_data.append({
                "sku_id": str(row[id_col]),
                "description": str(row[name_col]),
                "category": str(row[cat_col]),
                "brand": str(row[brand_col]) if brand_col in df.columns else "Generic",
                "current_price": round(float(row['price']), 2),
                "current_margin": round(float(row['margin_pct']), 1),
                "current_units": int(row['units']),
                "elasticity": elasticity,
                "abc_class": str(row['abc'])
            })

        result = {
            "total_sales": int(df['sales'].sum()),
            "raw_data": raw_data,
            "status": "success"
        }

        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
