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

# Σύνδεση με Supabase
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

        # --- ΣΤΑΘΕΡΟ MAPPING ΠΟΥ ΔΟΥΛΕΥΕΙ ---
        name_col = "SKU_De"       # Περιγραφή Προϊόντος
        sales_val_col = "Value Sales"  # Συνολικός Τζίρος (π.χ. 237.000€)
        
        # --- MAPPING ΤΙΜΩΝ ΒΑΣΕΙ ΟΔΗΓΙΩΝ ΣΟΥ ---
        retail_with_vat = "Sales_Price_With_VAT"  # Τιμή πώλησης ΜΕ ΦΠΑ (για εμφάνιση)
        retail_no_vat = "Sales_Without_VAT"     # Τιμή πώλησης ΧΩΡΙΣ ΦΠΑ (για Margin)
        cost_net = "Net_Price"                  # Τιμή αγοράς / Κόστος (Net Price)

        # Μετατροπή σε νούμερα
        df['sales_total'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['price_vat'] = pd.to_numeric(df[retail_with_vat], errors='coerce').fillna(0)
        df['price_no_vat'] = pd.to_numeric(df[retail_no_vat], errors='coerce').fillna(0)
        df['cost_net'] = pd.to_numeric(df[cost_net], errors='coerce').fillna(0)
        
        # Υπολογισμός Margin % (GM%) βασισμένος στις Net τιμές
        # Formula: ((Retail_No_Vat - Cost_Net) / Retail_No_Vat) * 100
        df['gm_percent'] = 0.0
        mask = df['price_no_vat'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'price_no_vat'] - df.loc[mask, 'cost_net']) / df.loc[mask, 'price_no_vat']) * 100
        
        # ABC Analysis (Σταθερή λογική)
        df = df.sort_values('sales_total', ascending=False)
        total_sales_sum = float(df['sales_total'].sum())
        
        if total_sales_sum > 0:
            df['cum_perc'] = (df['sales_total'].cumsum() / total_sales_sum) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        raw_data = []
        for _, row in df.iterrows():
            raw_data.append({
                "product_name": str(row[name_col]),
                "category": str(row["Segment"]) if "Segment" in df.columns else "General",
                "brand": str(row["Brand"]) if "Brand" in df.columns else "N/A",
                "sales": round(float(row['sales_total']), 2),
                "clean_sales_price": round(float(row['price_vat']), 2), # Εμφάνιση Τιμής ΜΕ ΦΠΑ
                "net_price": round(float(row['cost_net']), 2),           # Εμφάνιση Net Price (Κόστος)
                "gm_percent": round(float(row['gm_percent']), 1),        # Margin υπολογισμένο ΧΩΡΙΣ ΦΠΑ
                "abc_class": str(row['abc_class'])
            })

        result = {
            "total_sales": round(total_sales_sum, 2), 
            "raw_data": raw_data, 
            "status": "success"
        }

        # Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed", 
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({
                "analysis_status": "failed",
                "analysis_json": {"error": str(e)}
            }).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
