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

# Σύνδεση με Supabase
supabase: Client = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY")
)

def get_best_column(df, keywords):
    """Εντοπίζει τη στήλη που ταιριάζει καλύτερα στα keywords"""
    for col in df.columns:
        clean_col = str(col).strip().lower().replace(" ", "_")
        if any(key.lower() in clean_col for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        # Λήψη αρχείου
        response = requests.get(file_url, timeout=30)
        
        # --- ΘΩΡΑΚΙΣΜΕΝΗ ΑΝΑΓΝΩΣΗ EXCEL ---
        # Δοκιμάζουμε openpyxl (standard) και μετά fallback αν αποτύχει το encoding
        try:
            df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        except Exception:
            # Fallback για παλαιότερα formats ή encoding errors
            df = pd.read_excel(io.BytesIO(response.content))

        # Καθαρισμός ονομάτων στηλών από κενά και περίεργους χαρακτήρες
        df.columns = [str(c).strip() for c in df.columns]

        # 1. Mapping Στηλών
        sales_col = get_best_column(df, ["value_sales", "τζίρος", "sales_amount"])
        net_retail_col = get_best_column(df, ["sales_without_vat", "net_retail", "καθαρή_λιανική", "sales_price"])
        net_cost_col = get_best_column(df, ["net_price", "cost_price", "τιμή_αγοράς"])
        cat_col = get_best_column(df, ["category", "κατηγορία"])
        name_col = get_best_column(df, ["sku_desc", "description", "είδος", "product", "product_name"])
        brand_col = get_best_column(df, ["brand", "μάρκα"])

        # 2. Καθαρισμός Δεδομένων & Υπολογισμοί
        df['sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['clean_sales_price'] = pd.to_numeric(df[net_retail_col], errors='coerce').fillna(0) if net_retail_col else 0
        purchase_net = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0) if net_cost_col else 0
        
        # GM% Calculation
        df['gm_percent'] = 0
        mask = df['clean_sales_price'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'clean_sales_price'] - purchase_net) / df.loc[mask, 'clean_sales_price']) * 100
        df['gm_percent'] = df['gm_percent'].replace([np.inf, -np.inf], 0).fillna(0)

        # 3. ABC Analysis
        df = df.sort_values('sales', ascending=False)
        total_sales = float(df['sales'].sum())
        
        if total_sales > 0:
            df['cum_perc'] = (df['sales'].cumsum() / total_sales) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        # 4. Προετοιμασία JSON για το Lovable
        raw_data = []
        for _, row in df.iterrows():
            # Καθαρισμός ονόματος από μη-ascii χαρακτήρες αν χρειαστεί
            p_name = str(row[name_col]) if name_col else "Unknown"
            raw_data.append({
                "product_name": p_name,
                "category": str(row[cat_col]) if cat_col else "General",
                "brand": str(row[brand_col]) if brand_col else "N/A",
                "sales": round(float(row['sales']), 2),
                "clean_sales_price": round(float(row['clean_sales_price']), 2),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": str(row['abc_class'])
            })

        result = {
            "total_sales": round(total_sales, 2),
            "raw_data": raw_data,
            "status": "success"
        }

        # Ενημέρωση Supabase - Εδώ είναι το κλειδί!
        supabase.table("projects").update({
            "analysis_status": "completed", 
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}

    except Exception as e:
        print(f"Error detail: {str(e)}") # Για να το βλέπουμε στα logs του Render
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
