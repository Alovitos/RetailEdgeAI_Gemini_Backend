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
        price_col = get_best_column(df, ["Price", "Τιμή", "Rate", "Unit Price"])
        
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['unit_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0) if price_col else 0

        # 2. ABC Analysis (Σωστός υπολογισμός)
        df = df.sort_values(by='sales', ascending=False)
        df['cum_sales'] = df['sales'].cumsum()
        total_sum = df['sales'].sum()
        df['cum_percent'] = (df['cum_sales'] / total_sum) * 100
        
        df['abc_class'] = pd.cut(df['cum_percent'], bins=[0, 80, 95, 100.01], labels=['A', 'B', 'C'])
        
        abc_summary = df['abc_class'].value_counts().to_dict()
        abc_stats = [
            {"class": "A", "count": int(abc_summary.get('A', 0)), "share": 80},
            {"class": "B", "count": int(abc_summary.get('B', 0)), "share": 15},
            {"class": "C", "count": int(abc_summary.get('C', 0)), "share": 5}
        ]

        # 3. Price Range Analysis (Σωστά Bins)
        bins = [0, 5, 20, 50, 100, 10000]
        labels = ['0-5€', '5-20€', '20-50€', '50-100€', '100€+']
        df['price_range'] = pd.cut(df['unit_price'], bins=bins, labels=labels)
        price_summary = df.groupby('price_range')['sales'].sum().reset_index()
        price_analysis = [{"segment": row['price_range'], "value": float(row['sales'])} for index, row in price_summary.iterrows()]

        # 4. Final Result
        result = {
            "total_sales": round(float(total_sum), 2),
            "total_items": len(df),
            "abc_stats": abc_stats,
            "price_analysis": price_analysis,
            "raw_data": df.to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
