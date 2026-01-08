import pandas as pd
import io, os, requests, re
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def clean_string(text):
    return re.sub(r'[^a-z0-9]', '', str(text).lower())

def identify_sales_column(df):
    """Εντοπίζει τη στήλη τζίρου βάσει ονόματος ΚΑΙ μεγέθους τιμών"""
    potential_cols = []
    for col in df.columns:
        c_clean = clean_string(col)
        # Αν περιέχει sales/value/amount αλλά ΟΧΙ price/vat/unit
        if any(k in c_clean for k in ['sales', 'value', 'amount']) and \
           not any(nk in c_clean for nk in ['price', 'vat', 'unit', 'catalogue']):
            
            # Έλεγχος αν τα νούμερα είναι "μεγάλα" (άρα τζίρος και όχι τιμή)
            if pd.api.types.is_numeric_dtype(df[col]):
                avg_val = df[col].mean()
                if avg_val > 100: # Μια τιμή μονάδας σπάνια είναι > 100 στο retail σου
                    potential_cols.append((col, avg_val))
    
    if potential_cols:
        # Επιστρέφει τη στήλη με το μεγαλύτερο μέσο όρο (τον τζίρο)
        return sorted(potential_cols, key=lambda x: x[1], reverse=True)[0][0]
    return None

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id, file_url = body.get("project_id"), body.get("file_url")

        response = requests.get(file_url)
        df = pd.read_excel(io.BytesIO(response.content))
        
        # Έξυπνος εντοπισμός
        sales_col = identify_sales_column(df)
        # Αν αποτύχει ο έξυπνος, πάμε στη στήλη 'Value Sales' απευθείας
        if not sales_col:
            sales_col = next((c for c in df.columns if 'Value Sales' in c), None)

        total_sales = float(df[sales_col].sum()) if sales_col else 0
        
        summary = {
            "total_rows": int(len(df)),
            "total_sales": round(total_sales, 2),
            "detected_column": sales_col,
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
