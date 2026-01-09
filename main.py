import pandas as pd
import io, os, requests, np
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

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        # Λήψη αρχείου
        response = requests.get(file_url, timeout=60)
        file_data = io.BytesIO(response.content)
        
        # ΑΝΑΓΝΩΣΗ ΜΕ ΠΛΗΡΗ ΠΑΡΑΚΑΜΨΗ ENCODING ERRORS
        df = pd.read_excel(file_data, engine='openpyxl')
        
        # Καθαρισμός ονομάτων στηλών από κρυφούς χαρακτήρες
        df.columns = [str(c).strip().encode('ascii', 'ignore').decode('ascii') for c in df.columns]

        # --- ΑΥΣΤΗΡΟ EXACT MATCHING (image_2b6060.png) ---
        # Χρησιμοποιούμε τις ακριβείς ονομασίες που βλέπουμε στο Excel σου
        name_col = "SKU_De"
        sales_col = "Value Sales"
        net_cost_col = "Net_Price"
        retail_col = "Sales_Without_V"
        brand_col = "Brand"
        cat_col = "Segment"

        # Μετατροπή σε νούμερα με αφαίρεση τυχόν κειμένου
        df['sales_val'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['net_ret'] = pd.to_numeric(df[retail_col], errors='coerce').fillna(0)
        df['net_cost'] = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0)
        
        # Υπολογισμός Margin
        df['gm_percent'] = 0
        mask = df['net_ret'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'net_ret'] - df.loc[mask, 'net_cost']) / df.loc[mask, 'net_ret']) * 100
        df['gm_percent'] = df['gm_percent'].replace([np.inf, -np.inf], 0).fillna(0)

        # Κατάταξη ABC
        df = df.sort_values('sales_val', ascending=False)
        total_sales = float(df['sales_val'].sum())
        
        if total_sales > 0:
            df['cum_perc'] = (df['sales_val'].cumsum() / total_sales) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        raw_data = []
        for _, row in df.iterrows():
            # Καθαρίζουμε και το κείμενο του προϊόντος από περίεργα bytes
            p_name = str(row[name_col]).encode('ascii', 'ignore').decode('ascii')
            raw_data.append({
                "product_name": p_name,
                "category": str(row[cat_col]),
                "brand": str(row[brand_col]),
                "sales": round(float(row['sales_val']), 2),
                "clean_sales_price": round(float(row['net_ret']), 2),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": str(row['abc_class'])
            })

        result = {
            "total_sales": round(total_sales, 2),
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
