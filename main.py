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
            "sales": "Value Sales", "units": "Unit Sales", "price": "Sales_Price_Without_VAT", "net": "Net_Price"
        }

        for col in [mapping["sales"], mapping["units"], mapping["price"], mapping["net"]]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('€', '').str.replace(',', '').str.strip(), errors='coerce').fillna(0)

        df['gm_percent'] = np.where(df[mapping["price"]] > 0, ((df[mapping["price"]] - df[mapping["net"]]) / df[mapping["price"]]) * 100, 0)
        df = df.sort_values(mapping["sales"], ascending=False)
        total_sales_sum = df[mapping["sales"]].sum()
        df['cum_perc'] = (df[mapping["sales"]].cumsum() / (total_sales_sum + 0.01)) * 100
        df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C']).fillna('C')

        all_items = []
        for _, row in df.iterrows():
            # ΚΑΘΑΡΟ ΟΝΟΜΑ ΚΑΤΗΓΟΡΙΑΣ (Χωρίς ποσοστά)
            clean_cat = str(row[mapping["cat"]]).strip()
            
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
                "elasticity": -1.8
            }
            all_items.append(item_data)

        # Category Macro - Εδώ στέλνουμε το καθαρό όνομα για το φίλτρο
        cat_group = df.groupby(mapping["cat"]).agg({mapping["sales"]: 'sum'}).reset_index()
        category_macro = []
        for _, r in cat_group.iterrows():
            category_macro.append({
                "category": str(r[mapping["cat"]]).strip(),
                "sales": round(float(r[mapping["sales"]]), 2),
                "label": str(r[mapping["cat"]]).strip() # ΜΟΝΟ το όνομα, το % θα το βάλει το UI
            })

        result = {"items": all_items, "raw_data": all_items, "category_macro": category_macro, "status": "success"}
        supabase.table("projects").update({"analysis_json": result, "analysis_status": "completed"}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
