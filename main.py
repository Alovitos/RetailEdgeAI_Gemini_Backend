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

        # Το δικό σου mapping
        mapping = {
            "id": "SKU_ID", "desc": "SKU_Description", "brand": "Brand", "cat": "Category",
            "sales": "Value Sales", "units": "Unit Sales", "price": "Sales_Price_Without_VAT", "net": "Net_Price",
            "sales_ya": "Value Sales YA", "units_ya": "Unit Sales YA" # Νέες στήλες
        }

        # Καθαρισμός αριθμών (Value Sales, Units, Prices)
        cols_to_clean = [mapping["sales"], mapping["units"], mapping["price"], mapping["net"]]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').str.strip(), errors='coerce').fillna(0)

        # Καθαρισμός YA στηλών αν υπάρχουν
        for col in [mapping["sales_ya"], mapping["units_ya"]]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').str.strip(), errors='coerce').fillna(0)
            else:
                df[col] = 0

        # Υπολογισμοί Margin & ABC
        df['gm_percent'] = np.where(df[mapping["price"]] > 0, ((df[mapping["price"]] - df[mapping["net"]]) / df[mapping["price"]]) * 100, 0)
        df = df.sort_values(mapping["sales"], ascending=False)
        total_sales_sum = df[mapping["sales"]].sum()
        df['cum_perc'] = (df[mapping["sales"]].cumsum() / (total_sales_sum + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        # Υπολογισμός Category Benchmarks (Μέσος όρος Margin ανά Κατηγορία)
        cat_benchmarks = {}
        for cat in df[mapping["cat"]].unique():
            cat_df = df[df[mapping["cat"]] == cat]
            total_rev = cat_df[mapping["sales"]].sum()
            total_cost = (cat_df[mapping["units"]] * cat_df[mapping["net"]]).sum()
            margin = ((total_rev - total_cost) / (total_rev + 0.01)) * 100
            cat_benchmarks[str(cat).strip()] = round(float(margin), 1)

        all_items = []
        for _, row in df.iterrows():
            clean_cat = str(row[mapping["cat"]]).strip()
            
            # Υπολογισμός Growth
            sales_ya = float(row[mapping["sales_ya"]])
            sales_growth = round(((float(row[mapping["sales"]]) - sales_ya) / sales_ya * 100), 1) if sales_ya > 0 else 0
            
            units_ya = float(row[mapping["units_ya"]])
            vol_growth = round(((float(row[mapping["units"]]) - units_ya) / units_ya * 100), 1) if units_ya > 0 else 0

            item_data = {
                "sku_id": str(row[mapping["id"]]),
                "name": str(row[mapping["desc"]]),
                "description": str(row[mapping["desc"]]),
                "category": clean_cat,
                "brand": str(row[mapping["brand"]]),
                "revenue": float(row[mapping["sales"]]),
                "sales": float(row[mapping["sales"]]),
                "units": int(row[mapping["units"]]),
                "price": round(float(row[mapping["price"]]), 2),
                "net_price": round(float(row[mapping["net"]]), 2),
                "gm_percent": round(float(row['gm_percent']), 1),
                "abc_class": str(row['abc_class']),
                "smart_tag": "Star Product" if float(row['gm_percent']) > 25 else "Maintain",
                "elasticity": -1.8,
                "sales_growth": sales_growth,
                "volume_growth": vol_growth
            }
            all_items.append(item_data)

        cat_group = df.groupby(mapping["cat"]).agg({mapping["sales"]: 'sum'}).reset_index()
        category_macro = []
        for _, r in cat_group.iterrows():
            cat_name = str(r[mapping["cat"]]).strip()
            category_macro.append({
                "category": cat_name,
                "sales": round(float(r[mapping["sales"]]), 2),
                "label": cat_name,
                "avg_margin": cat_benchmarks.get(cat_name, 0) # Προσθήκη του benchmark
            })

        result = {
            "items": all_items, 
            "raw_data": all_items, 
            "category_macro": category_macro, 
            "category_benchmarks": cat_benchmarks, # Νέο πεδίο για το Negotiation Hub
            "status": "success"
        }
        
        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
