import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_best_column(df_columns, keywords, negative_keywords=[]):
    """Αλγόριθμος βαθμολόγησης στηλών"""
    best_col = None
    max_score = -1
    for col in df_columns:
        score = 0
        c_low = col.lower()
        # Αν περιέχει θετική λέξη-κλειδί, παίρνει πόντους
        for k in keywords:
            if k in c_low: score += 10
        # Αν περιέχει αρνητική λέξη (π.χ. 'price', 'unit'), χάνει πόντους για τον Τζίρο
        for nk in negative_keywords:
            if nk in c_low: score -= 15
        
        if score > max_score:
            max_score = score
            best_col = col
    return best_col if max_score > 0 else None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id, file_url = body.get("project_id"), body.get("file_url")

        response = requests.get(file_url)
        df = pd.read_excel(io.BytesIO(response.content))
        
        # 1. Εντοπισμός Τζίρου (Value Sales) - Αποφεύγουμε το 'Price'
        sales_col = get_best_column(df.columns, 
                                   ['value sales', 'total sales', 'τζίρος', 'καθαρές πωλήσεις', 'amount'], 
                                   ['price', 'unit', 'τιμή', 'vat'])

        # 2. Εντοπισμός Όγκου (Volume)
        vol_col = get_best_column(df.columns, 
                                 ['baseline_sales', 'volume', 'qty', 'ποσότητα', 'τεμάχια', 'units'])

        # 3. Εντοπισμός Brand
        brand_col = get_best_column(df.columns, ['brand', 'μάρκα', 'vendor', 'κατασκευαστής'])

        # Υπολογισμοί
        total_sales = float(df[sales_col].sum()) if sales_col else 0
        total_vol = float(df[vol_col].sum()) if vol_col else 0
        
        # Top 5 Brands Insight
        brand_data = {}
        if brand_col and sales_col:
            brand_data = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5).to_dict()

        summary = {
            "total_rows": int(len(df)),
            "total_sales": round(total_sales, 2),
            "total_volume": round(total_vol, 2),
            "detected_sales_column": sales_col,
            "detected_volume_column": vol_col,
            "brand_distribution": brand_data,
            "all_columns": df.columns.tolist()
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
