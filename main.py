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

        mapping = {
            "id": "SKU_ID", "desc": "SKU_Description", "brand": "Brand", "cat": "Category",
            "sales": "Value Sales", "units": "Unit Sales", "price": "Sales_Price_Without_VAT", "net": "Net_Price",
            "sales_ya": "Value Sales YA", "units_ya": "Unit Sales YA"
        }

        # Καθαρισμός αριθμών
        for col in [mapping["sales"], mapping["units"], mapping["price"], mapping["net"], mapping["sales_ya"], mapping["units_ya"]]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').str.strip(), errors='coerce').fillna(0)
            else:
                df[col] = 0

        # Υπολογισμός Margin SKU βάσει τιμών (ΟΧΙ από τη στήλη του Excel)
        # Margin % = ((Price - Net) / Price) * 100
        df['gm_percent'] = np.where(df[mapping["price"]] > 0, 
                                   ((df[mapping["price"]] - df[mapping["net"]]) / df[mapping["price"]]) * 100, 0)

        # Υπολογισμός Category Benchmarks (Weighted Average Margin)
        # Weighting per SKU = (Price - Net) * Units
        cat_benchmarks = {}
        for cat in df[mapping["cat"]].unique():
            cat_df = df[df[mapping["cat"]] == cat]
            total_cat_sales = cat_df[mapping["sales"]].sum()
            
            # Συνολικό Profit σε Ευρώ για την κατηγορία
            total_profit_euro = ((cat_df[mapping["price"]] - cat_df[mapping["net"]]) * cat_df[mapping["units"]]).sum()
            
            if total_cat_sales > 0:
                weighted_margin = (total_profit_euro / total_cat_sales) * 100
            else:
                weighted_margin = 0
            
            cat_benchmarks[str(cat).strip()] = round(float(weighted_margin), 1)

        all_items = []
        for _, row in df.iterrows():
            clean_cat = str(row[mapping["cat"]]).strip()
            
            # Growth Logic
            sales_ya = float(row[mapping["sales_ya"]])
            sales_growth = round(((float(row[mapping["sales"]]) - sales_ya) / sales_ya * 100), 1) if sales_ya > 0 else 0
            
            item_data = {
                "sku_id": str(row[mapping["id"]]),
                "name": str(row[mapping["desc"]]),
                "category": clean_cat,
                "brand": str(row[mapping["brand"]]),
                "sales": float(row[mapping["sales"]]),
                "units": int(row[mapping["units"]]),
                "price": round(float(row[mapping["price"]]), 2),
                "net_price": round(float(row[mapping["net"]]), 2),
                "gm_percent": round(float(row['gm_percent']), 1),
                "abc_class": "N/A", # Θα μπορούσε να υπολογιστεί δυναμικά
                "sales_growth": sales_growth,
                "target_margin": cat_benchmarks.get(clean_cat, 0)
            }
            all_items.append(item_data)

        result = {
            "items": all_items, 
            "category_benchmarks": cat_benchmarks,
            "status": "success"
        }
        
        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
