import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def get_best_column(df, keywords):
    """Βρίσκει την πιο ταιριαστή στήλη βάσει λέξεων-κλειδιών"""
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

        # Λήψη και ανάγνωση
        response = requests.get(file_url, timeout=30)
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # 1. Εντοπισμός στηλών με προτεραιότητα
        # Ψάχνουμε για "Total Sales" ή "Value" για να αποφύγουμε την απλή τιμή μονάδας
        sales_col = get_best_column(df, ["Total Sales", "Συνολικές Πωλήσεις", "Value Sales", "Τζίρος"])
        if not sales_col: # Αν δεν βρει τα παραπάνω, ψάχνει γενικά για πωλήσεις
            sales_col = get_best_column(df, ["Sales", "Πωλήσεις", "Value"])
        
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        qty_col = get_best_column(df, ["Qty", "Quantity", "Ποσότητα", "Units"])

        # 2. Υπολογισμοί
        total_sales = pd.to_numeric(df[sales_col], errors='coerce').sum() if sales_col else 0
        total_qty = pd.to_numeric(df[qty_col], errors='coerce').sum() if qty_col else len(df)
        
        # 3. Top 5 Brands για το γράφημα
        top_brands = []
        if brand_col and sales_col:
            brand_summary = df.groupby(brand_col)[sales_col].sum().sort_values(ascending=False).head(5)
            top_brands = [{"name": str(k), "value": float(v)} for k, v in brand_summary.items()]

        result = {
            "total_sales": round(float(total_sales), 2),
            "total_volume": int(total_qty),
            "top_brands": top_brands,
            "detected_columns": {"sales": sales_col, "brand": brand_col, "qty": qty_col}
        }

        # Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}
    except Exception as e:
        supabase.table("projects").update({"analysis_status": "failed", "analysis_json": {"error": str(e)}}).eq("id", project_id).execute()
        return {"status": "error"}
