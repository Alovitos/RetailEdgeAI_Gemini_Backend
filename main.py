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
    for col in df.columns:
        if any(key.lower() in str(col).lower() for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # 1. Mapping
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος", "Value"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα", "Επωνυμία", "Manufacturer"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group", "Ομάδα", "Department"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός", "Item No"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name", "Προϊόν"])
        price_col = get_best_column(df, ["Price", "Τιμή", "Rate"])

        # 2. Προετοιμασία Δεδομένων
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['brand'] = df[brand_col].astype(str).str.strip() if brand_col else "N/A"
        df['category'] = df[cat_col].astype(str).str.strip() if cat_col else "N/A"
        df['unit_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0) if price_col else 0
        
        if code_col and desc_col:
            df['product'] = df[code_col].astype(str) + " - " + df[desc_col].astype(str)
        else:
            df['product'] = df[desc_col] if desc_col else "Unknown"

        # 3. ABC Analysis Logic
        df_sorted = df.sort_values(by='sales', ascending=False)
        df_sorted['cum_sales'] = df_sorted['sales'].cumsum()
        total_sum = df_sorted['sales'].sum()
        df_sorted['cum_percent'] = 100 * df_sorted['cum_sales'] / total_sum
        
        def abc_classify(percent):
            if percent <= 80: return 'A'
            elif percent <= 95: return 'B'
            else: return 'C'
            
        df['abc_class'] = df_sorted['cum_percent'].apply(abc_classify)

        # 4. Price Segments
        price_bins = [0, 10, 50, 100, 500, 10000]
        price_labels = ['0-10€', '10-50€', '50-100€', '100-500€', '500€+']
        df['price_segment'] = pd.cut(df['unit_price'], bins=price_bins, labels=price_labels, include_lowest=True)

        # 5. Στατιστικά για το JSON
        abc_counts = df['abc_class'].value_counts().to_dict()
        price_segments = df.groupby('price_segment')['sales'].sum().to_dict()

        result = {
            "total_sales": round(float(total_sum), 2),
            "abc_stats": [{"class": k, "count": int(v)} for k, v in abc_counts.items()],
            "price_analysis": [{"segment": str(k), "value": float(v)} for k, v in price_segments.items()],
            "filters": {
                "brands": sorted(df['brand'].unique().tolist()),
                "categories": sorted(df['category'].unique().tolist()),
                "abc_class": ['A', 'B', 'C']
            },
            "raw_data": df[['brand', 'category', 'product', 'sales', 'abc_class', 'price_segment', 'unit_price']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error"}
