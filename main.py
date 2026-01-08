import pandas as pd
import io, os, requests, re
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def clean_string(text):
    """Καθαρίζει το κείμενο από κενά, underscores και το κάνει πεζά"""
    return re.sub(r'[^a-z0-9]', '', str(text).lower())

def get_best_column(df_columns, keywords, negative_keywords=[]):
    best_col = None
    max_score = -1
    
    # Καθαρισμός αρνητικών λέξεων
    clean_negatives = [clean_string(nk) for nk in negative_keywords]
    
    for col in df_columns:
        score = 0
        c_clean = clean_string(col)
        
        # Αν η στήλη περιέχει ακριβώς τη φράση "valuesales" παίρνει τεράστιο προβάδισμα
        for k in keywords:
            k_clean = clean_string(k)
            if k_clean in c_clean:
                score += 20
                # Bonus αν είναι ακριβώς ίδια
                if k_clean == c_clean: score += 10
        
        # Ποινή αν περιέχει "price", "unit", "vat", "catalogue"
        for nk in clean_negatives:
            if nk in c_clean:
                score -= 40  # Μεγάλη ποινή για να διώξουμε τις τιμές
        
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
        # Διαβάζουμε το Excel
        df = pd.read_excel(io.BytesIO(response.content))
        
        # 1. Εντοπισμός Τζίρου (Value Sales) - Σκληρό φιλτράρισμα κατά των τιμών
        sales_col = get_best_column(df.columns, 
                                   ['valuesales', 'totalsales', 'amount', 'revenue'], 
                                   ['price', 'unit', 'vat', 'catalogue', 'netprice'])

        # 2. Εντοπισμός Όγκου (Volume)
        vol_col = get_best_column(df.columns, 
                                 ['baseline', 'volume', 'qty', 'units', 'τεμάχια'])

        # 3. Εντοπισμός Brand
        brand_col = get_best_column(df.columns, ['brand', 'μάρκα', 'vendor'])

        # Υπολογισμοί
        total_sales = float(df[sales_col].sum()) if sales_col else 0
        total_vol = float(df[vol_col].sum()) if vol_col else 0
        
        brand_data = {}
        if brand_col and sales_col:
            brand_data = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5).to_dict()

        summary = {
            "total_rows": int(len(df)),
            "total_sales": round(total_sales, 2),
            "total_volume": round(total_vol, 2),
            "detected_sales_column": sales_col,
            "detected_volume_column": vol_col,
            "brand_distribution": brand_data
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": summary
        }).eq("id", project_id).execute()
        
        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
