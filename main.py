import pandas as pd
import io, os, requests
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

def get_best_column(df, keywords, exclude_keywords=None):
    """Εντοπίζει στήλη με βάση keywords, αποφεύγοντας συγκεκριμένες λέξεις (π.χ. Price)"""
    for col in df.columns:
        clean_col = str(col).strip().lower()
        # Έλεγχος αν η στήλη περιέχει τα keywords
        if any(key.lower() in clean_col for key in keywords):
            # Έλεγχος αν πρέπει να εξαιρέσουμε τη στήλη (π.χ. αν περιέχει τη λέξη Price)
            if exclude_keywords and any(ex.lower() in clean_col for ex in exclude_keywords):
                continue
            return col
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=60)
        file_data = io.BytesIO(response.content)
        
        try:
            df = pd.read_excel(file_data, engine='openpyxl')
        except:
            df = pd.read_excel(file_data)

        df.columns = [str(c).strip() for c in df.columns]

        # --- ΑΥΣΤΗΡΟ MAPPING ΣΤΗΛΩΝ ---
        # Για τα Sales: Ψάχνουμε 'Value' ή 'Sales' αλλά ΑΠΟΚΛΕΙΟΥΜΕ το 'Price'
        sales_col = get_best_column(df, ["value_sales", "total_sales", "τζίρος"], exclude_keywords=["price", "τιμή"])
        if not sales_col: # Fallback αν δεν βρει τα παραπάνω
            sales_col = get_best_column(df, ["sales", "revenue"], exclude_keywords=["price", "unit"])

        # Για τις Τιμές: Εδώ επιτρέπουμε το 'Price'
        net_retail_col = get_best_column(df, ["net_retail", "sales_price", "τιμή_λιανικής"])
        net_cost_col = get_best_column(df, ["net_price", "cost_price", "τιμή_αγοράς"])
        
        name_col = get_best_column(df, ["sku_desc", "description", "product_name", "είδος"])
        cat_col = get_best_column(df, ["category", "κατηγορία"])
        brand_col = get_best_column(df, ["brand", "μάρκα"])

        # Data Cleaning
        df['sales_val'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['net_ret'] = pd.to_numeric(df[net_retail_col], errors='coerce').fillna(0) if net_retail_col else 0
        df['net_cost'] = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0) if net_cost_col else 0
        
        # GM%
        df['gm_percent'] = 0
        mask = df['net_ret'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'net_ret'] - df.loc[mask, 'net_cost']) / df.loc[mask, 'net_ret']) * 100
        df['gm_percent'] = df['gm_percent'].replace([np.inf, -np.inf], 0).fillna(0)

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
            raw_data.append({
                "product_name": str(row[name_col]) if name_col else "Unknown",
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
