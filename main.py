from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from supabase import create_client, Client
import pandas as pd
import io
import os
import requests

# 1. Ρυθμίσεις (Θα τα πάρει από το Render)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

app = FastAPI()

# Σύνδεση με Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Τι περιμένουμε να λάβουμε από το Lovable
class AnalysisRequest(BaseModel):
    project_id: str
    file_url: str

@app.get("/")
def read_root():
    return {"status": "RetailEdge Backend is running"}

@app.post("/analyze")
def analyze_excel(request: AnalysisRequest):
    try:
        print(f"Starting analysis for project: {request.project_id}")
        
        # 2. Κατέβασμα του Excel από το URL
        response = requests.get(request.file_url)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Could not download file")
        
        # 3. Ανάγνωση με Pandas
        file_content = io.BytesIO(response.content)
        df = pd.read_excel(file_content)
        
        # Καθαρισμός ονομάτων στηλών (μικρά γράμματα, χωρίς κενά)
        df.columns = df.columns.str.strip().str.lower()
        
        # --- ΕΔΩ ΓΙΝΕΤΑΙ Η ΜΑΓΕΙΑ ΤΟΥ NRM (Προς το παρόν κάνουμε ένα απλό Summary) ---
        # Θα ψάξουμε για στήλες που μοιάζουν με 'sales', 'revenue', 'price', 'volume'
        
        summary = {
            "total_rows": len(df),
            "columns_found": df.columns.tolist(),
            "numeric_summary": {}
        }
        
        # Βρες στήλες με νούμερα και κάνε άθροισμα
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            summary["numeric_summary"][col] = df[col].sum()

        # 4. Αποθήκευση αποτελεσμάτων πίσω στο Supabase
        data_to_save = {
            "status": "completed",
            "analysis_json": summary
        }
        
        update_response = supabase.table("projects").update(data_to_save).eq("id", request.project_id).execute()
        
        return {"status": "success", "data": summary}

    except Exception as e:
        print(f"Error: {str(e)}")
        # Αν αποτύχει, ενημέρωσε τη βάση
        supabase.table("projects").update({"status": "failed"}).eq("id", request.project_id).execute()
        raise HTTPException(status_code=500, detail=str(e))
