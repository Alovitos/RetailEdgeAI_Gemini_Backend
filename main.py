import pandas as pd
import io, os, requests
import numpy as np
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_best_column(df, keywords):
    """Εντοπίζει τη στήλη που ταιριάζει καλύτερα στα keywords"""
    for col in df.columns:
        if any(key.lower() == str(col).lower().replace(" ", "_") for key in keywords) or \
           any(key.lower() in str(col).lower() for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        
        # Καθαρισμός ονομάτων στηλών (αφαίρεση κενών)
        df.columns = [str(c).strip() for c in df.columns]

        # 1. ΑΚΡΙΒΕΣ MAPPING (βάσει του image_1bf323.png)
        sales_col = get_best_column(df, ["Value_Sales", "Value Sales", "Τζίρος"])
        net_retail_col = get_best_column(df, ["Sales_Without_VAT", "Sales Without VAT", "Net_Retail"])
        net_cost_col = get_best_column(df, ["Net_Price", "Net Price", "Cost_Price"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία"])
        name_col = get_best_column(df, ["SKU_Desc", "Description", "Είδος"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])

        # 2. Υπολογισμοί χωρίς υποθέσεις
        # Παίρνουμε απευθείας την καθαρή τιμή πώλησης από το αρχείο
        df['clean_sales_price'] = pd.to_numeric(df[net_retail_col], errors='coerce').fillna(0)
        purchase_net = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0)
        
        # GM% = (Net Retail - Net Cost) / Net Retail
        df['gm_percent'] = ((df['clean_sales_price'] - purchase_net) / df['clean_sales_price'].replace(0, np.nan)) * 100
        df['gm_percent'] = df['gm_percent'].fillna(0).replace([np.inf, -np.inf], 0)

        # 3. Προετοιμασία Data για το Lovable
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['category'] = df[cat_col].astype(str) if cat_col else "General"
        df['brand'] = df[brand_col].astype(str) if brand_col else "N/A"
        df['product_name'] = df[name_col].astype(str) if name_col else "Unknown"

        # ABC Analysis (Dynamic)
        def calculate_abc(group):
            group = group.sort_values('sales', ascending=False)
            total = group['sales'].sum()
            if total <= 0: return group.assign(abc_class='C')
            cum_pct = (group['sales'].cumsum() / total) * 100
            group['abc_class'] = pd.cut(cum_pct, bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
            return group

        df = df.groupby('category', group_keys=False).apply(calculate_abc)

        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "raw_data": df[['product_name', 'category', 'brand', 'sales', 'clean_sales_price', 'abc_class', 'gm_percent']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
