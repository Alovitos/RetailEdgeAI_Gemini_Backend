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

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_smart_elasticity(category_name, product_name):
    """Επιστρέφει Price Elasticity βάσει FMCG βιβλιογραφίας (Ελληνικά/Αγγλικά)"""
    text = (str(category_name) + " " + str(product_name)).lower()
    
    # ΠΑΝΕΣ / ΒΡΕΦΙΚΑ (Ανελαστικά)
    if any(x in text for x in ['baby', 'diaper', 'pampers', 'nappy', 'πάνες', 'βρεφικά', 'μωρό']):
        return -0.6
    # SNACKS / CHIPS / ΠΑΓΩΤΑ (Πολύ Ελαστικά)
    if any(x in text for x in ['ice cream', 'snack', 'chocolate', 'chips', 'lays', 'παγωτό', 'τσιπς', 'γαριδάκια']):
        return -3.2
    # ΓΙΑΟΥΡΤΙ / ΤΥΡΙ / ΓΑΛΑΚΤΟΚΟΜΙΚΑ (Μεσαία)
    if any(x in text for x in ['yogurt', 'cheese', 'milk', 'γιαούρτι', 'τυρί', 'γάλα']):
        return -1.5
    # DEFAULT για τα υπόλοιπα
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

        # --- MAPPING ΣΤΗΛΩΝ ---
        name_col = "SKU_Description"
        sales_val_col = "Value Sales"
        unit_sales_col = "Unit Sales"  # Η νέα στήλη
        retail_with_vat = "Sales_Price_With_VAT"
        retail_no_vat = "Sales_Without_VAT"
        cost_net = "Net_Price"
        segment_col = "Segment" if "Segment" in df.columns else None

        # Μετατροπή σε αριθμούς
        df['sales_total'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['units_total'] = pd.to_numeric(df[unit_sales_col], errors='coerce').fillna(0)
        df['price_vat'] = pd.to_numeric(df[retail_with_vat], errors='coerce').fillna(0)
        df['price_no_vat'] = pd.to_numeric(df[retail_no_vat], errors='coerce').fillna(0)
        df['cost_net'] = pd.to_numeric(df[cost_net], errors='coerce').fillna(0)
        
        # Υπολογισμός Margin %
        df['gm_percent'] = 0.0
        mask = df['price_no_vat'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'price_no_vat'] - df.loc[mask, 'cost_net']) / df.loc[mask, 'price_no_vat']) * 100
        
        # ABC Analysis
        df = df.sort_values('sales_total', ascending=False)
        total_sales_sum = float(df['sales_total'].sum())
        
        if total_sales_sum > 0:
            df['cum_perc'] = (df['sales_total'].cumsum() / total_sales_sum) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        raw_data = []
        for _, row in df.iterrows():
            p_name = str(row[name_col])
            p_cat = str(row[segment_col]) if segment_col else ""
            
            raw_data.append({
                "product_name": p_name,
                "category": p_cat,
                "units": int(row['units_total']), # ΝΕΟ ΠΕΔΙΟ
                "sales": int(round(float(row['sales_total']), 0)), 
                "clean_sales_price": round(float(row['price_vat']), 2),
                "net_price": round(float(row['cost_net']), 2),
                "gm_percent": round(float(row['gm_percent']), 1),
                "abc_class": str(row['abc_class']),
                "suggested_elasticity": float(get_smart_elasticity(p_cat, p_name))
            })

        result = {"total_sales": int(round(total_sales_sum, 0)), "raw_data": raw_data, "status": "success"}
        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
