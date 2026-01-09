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

def find_smart_columns(df):
    """Έξυπνος εντοπισμός στηλών με βάση το περιεχόμενο και το όνομα"""
    col_map = {}
    
    # 1. Εντοπισμός Στήλης Πωλήσεων (Τζίρος) - Η πιο σημαντική
    best_sales_score = -1
    for col in df.columns:
        score = 0
        name = str(col).lower()
        # Keywords
        if any(k in name for k in ['sales', 'value', 'revenue', 'τζίρος', 'πωλήσεις']): score += 10
        if any(k in name for k in ['price', 'unit', 'τιμή', 'λιανική']): score -= 15 # Αποκλείουμε τιμές
        
        # Έλεγχος δεδομένων: Ο τζίρος έχει συνήθως μεγάλα αθροίσματα
        vals = pd.to_numeric(df[col], errors='coerce').dropna()
        if not vals.empty:
            if vals.max() > 500: score += 5 # Αν έχει μεγάλα νούμερα είναι τζίρος
            if vals.mean() > 50: score += 5
        
        if score > best_sales_score:
            best_sales_score = score
            col_map['sales'] = col

    # 2. Εντοπισμός Στήλης Ονόματος
    for col in df.columns:
        name = str(col).lower()
        if any(k in name for k in ['sku', 'desc', 'product', 'είδος', 'περιγραφή']):
            col_map['name'] = col
            break

    # 3. Εντοπισμός Τιμής και Κόστους
    for col in df.columns:
        name = str(col).lower()
        if any(k in name for k in ['net_retail', 'sales_price', 'καθαρή']): col_map['retail'] = col
        if any(k in name for k in ['cost', 'αγοράς', 'net_price']): col_map['cost'] = col
        if any(k in name for k in ['category', 'κατηγορία']): col_map['cat'] = col
        if any(k in name for k in ['brand', 'μάρκα']): col_map['brand'] = col

    return col_map

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=60)
        df = pd.read_excel(io.BytesIO(response.content))
        df.columns = [str(c).strip() for c in df.columns]

        # Χρήση της έξυπνης συνάρτησης
        cmap = find_smart_columns(df)
        
        # Υπολογισμοί με ασφάλεια
        sales = pd.to_numeric(df[cmap.get('sales')], errors='coerce').fillna(0)
        retail = pd.to_numeric(df[cmap.get('retail')], errors='coerce').fillna(0) if cmap.get('retail') else 0
        cost = pd.to_numeric(df[cmap.get('cost')], errors='coerce').fillna(0) if cmap.get('cost') else 0
        
        # GM% και ABC
        gm = np.where(retail > 0, ((retail - cost) / retail) * 100, 0)
        
        temp_df = pd.DataFrame({
            'name': df[cmap.get('name', df.columns[0])].astype(str),
            'sales': sales,
            'gm': gm,
            'retail': retail,
            'cat': df[cmap.get('cat', 'General')].astype(str) if cmap.get('cat') in df.columns else 'General',
            'brand': df[cmap.get('brand', 'N/A')].astype(str) if cmap.get('brand') in df.columns else 'N/A'
        }).sort_values('sales', ascending=False)

        total_sales = float(temp_df['sales'].sum())
        if total_sales > 0:
            temp_df['cum'] = (temp_df['sales'].cumsum() / total_sales) * 100
            temp_df['abc'] = pd.cut(temp_df['cum'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            temp_df['abc'] = 'C'

        raw_data = []
        for _, row in temp_df.iterrows():
            raw_data.append({
                "product_name": row['name'],
                "category": row['cat'],
                "brand": row['brand'],
                "sales": round(float(row['sales']), 2),
                "clean_sales_price": round(float(row['retail']), 2),
                "gm_percent": round(float(row['gm']), 2),
                "abc_class": str(row['abc'])
            })

        result = {"total_sales": round(total_sales, 2), "raw_data": raw_data, "status": "success"}
        supabase.table("projects").update({"analysis_status": "completed", "analysis_json": result}).eq("id", project_id).execute()
        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
