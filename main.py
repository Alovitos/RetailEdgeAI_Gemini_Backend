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

        # 1. Mapping Στηλών
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος", "Value"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα", "Επωνυμία", "Manufacturer"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group", "Ομάδα", "Department"])
        code_col = get_best_column(df, ["SKU", "Code", "Κωδικός", "Item No"])
        desc_col = get_best_column(df, ["Description", "Περιγραφή", "Name", "Προϊόν"])

        # 2. Προετοιμασία Σταθερών Στηλών για το Frontend
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['brand'] = df[brand_col].astype(str).str.strip() if brand_col else "N/A"
        df['category'] = df[cat_col].astype(str).str.strip() if cat_col else "N/A"
        
        if code_col and desc_col:
            df['product'] = df[code_col].astype(str) + " - " + df[desc_col].astype(str)
        else:
            df['product'] = df[desc_col] if desc_col else (df[code_col] if code_col else "Unknown")

        # 3. Φίλτρα (Unique Values)
        filters = {
            "brands": sorted(df['brand'].unique().tolist()),
            "categories": sorted(df['category'].unique().tolist()),
            "products": sorted(df['product'].unique().tolist())
        }

        # 4. JSON Result με σταθερή δομή
        result = {
            "total_sales": round(float(df['sales'].sum()), 2),
            "filters": filters,
            "raw_data": df[['brand', 'category', 'product', 'sales']].to_dict(orient='records'),
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
