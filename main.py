import pandas as pd
import io, os, requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI()

# Ρύθμιση CORS για να μπορεί το Lovable να μιλάει με το Render
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Σύνδεση με Supabase
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

@app.post("/analyze")
async def analyze_excel(request: Request):
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        if not file_url or not project_id:
            return {"status": "error", "message": "Missing data"}

        # 1. Κατέβασμα αρχείου
        response = requests.get(file_url)
        file_content = io.BytesIO(response.content)
        
        # 2. Ανάγνωση Excel με το σωστό engine
        # Εδώ λύνουμε το σφάλμα που είδες στις φωτογραφίες
        df = pd.read_excel(file_content, engine='openpyxl')
        
        # 3. Καθαρισμός ονομάτων στηλών (αφαίρεση κενών)
        df.columns = [str(c).strip() for c in df.columns]

        # 4. Εύρεση στήλης πωλήσεων (Max Sum logic)
        numeric_sums = {}
        for col in df.columns:
            series = pd.to_numeric(df[col], errors='coerce')
            if series.notna().any():
                numeric_sums[col] = float(series.sum())

        # Επιλογή: Προτίμηση στο 'Value Sales', αλλιώς η στήλη με το μεγαλύτερο άθροισμα
        sales_col = "Value Sales" if "Value Sales" in numeric_sums else None
        if not sales_col and numeric_sums:
            sales_col = max(numeric_sums, key=numeric_sums.get)
            
        final_sales = numeric_sums.get(sales_col, 0) if sales_col else 0

        # 5. Δημιουργία αποτελέσματος
        analysis_result = {
            "total_sales": round(final_sales, 2),
            "total_volume": int(len(df)),
            "detected_column": sales_col,
            "status": "success"
        }

        # 6. Ενημέρωση Supabase
        supabase.table("projects").update({
            "analysis_status": "completed",
            "analysis_json": analysis_result
        }).eq("id", project_id).execute()
        
        return {"status": "success", "data": analysis_result}

    except Exception as e:
        # Καταγραφή του σφάλματος στη βάση για να το βλέπουμε
        error_msg = str(e)
        supabase.table("projects").update({
            "analysis_status": "failed",
            "analysis_json": {"error": error_msg}
        }).eq("id", project_id).execute()
        return {"status": "error", "message": error_msg}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
