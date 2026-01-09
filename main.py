import pandas as pd
import io, os, requests
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
        brand_col = get_best_column(df, ["Brand", "Μάρκα", "Επωνυμία"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name"])

        # 2. Καθαρισμός & Προετοιμασία
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['unit_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0) if price_col else 0
        df['brand'] = df[brand_col].astype(str).str.strip() if brand_col else "N/A"
        df['category'] = df[cat_col].astype(str).str.strip() if cat_col else "N/A"
        
        if code_col and desc_col:
            df['product'] = df[code_col].astype(str) + " - " + df[desc_col].astype(str)
        else:
            df['product'] = df[desc_col] if desc_col else "Unknown"

        # 3. ABC Analysis (Υπολογισμός ανά τεμάχιο/γραμμή)
        df = df.sort_values(by='sales', ascending=False).reset_index(drop=True)
        df['cum_sales'] = df['sales'].cumsum()
        total_sum = df['sales'].sum()
        df['cum_percent'] = (df['cum_sales'] / total_sum) * 100

        # Ορισμός ορίων
        def classify_abc(row):
            if row['cum_percent'] <= 70: return 'A'
            elif row['cum_percent'] <= 90: return 'B'
            else: return 'C'
        
        df['abc_class'] = df.apply(classify_abc, axis=1)

        # 4. Σύνοψη για τα γραφήματα
        abc_summary = df['abc_class'].value_counts().to_dict()
        abc_stats = [
            {"name": "Class A (High Value)", "value": int(abc_summary.get('A', 0)), "color": "#22c55e"},
            {"name": "Class B (Medium Value)", "value": int(abc_summary.get('B', 0)), "color": "#f59e0b"},
            {"name": "Class C (Low Value)", "value": int(abc_summary.get('C', 0)), "color": "#ef4444"}
        ]

        result = {
            "total_sales": round(float(total_sum), 2),
            "total_items": len(df),
            "abc_stats": abc_stats,
            "raw_data": df[['brand', 'category', 'product', 'sales', 'abc_class', 'unit_price']].to_dict(orient='records'),
            "status": "success"
        }

        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
