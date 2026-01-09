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

@app.post("/analyze")
async def analyze_excel(request: Request):
    project_id = None
    try:
        body = await request.json()
        project_id = body.get("project_id")
        file_url = body.get("file_url")

        response = requests.get(file_url, timeout=60)
        # Χρήση openpyxl και ανάγνωση όλων των στηλών ως strings στην αρχή για ασφάλεια
        df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]

        # --- ΑΥΣΤΗΡΟ MAPPING ΒΑΣΕΙ ΤΟΥ EXCEL ΣΟΥ (image_2b6060.png) ---
        
        # 1. Product Name -> SKU_De (αντί για SKU_ID)
        name_col = "SKU_De" if "SKU_De" in df.columns else df.columns[0]
        
        # 2. Value Sales -> Η στήλη "Value Sales" (ΟΧΙ η Weekly_Baseline)
        # Ψάχνουμε για ακριβή αντιστοιχία για να αποφύγουμε το "Weekly_Baseline"
        sales_col = "Value Sales" if "Value Sales" in df.columns else None
        if not sales_col:
            # Αν δεν υπάρχει το ακριβές, ψάχνουμε κάτι που περιέχει 'Value' αλλά ΟΧΙ 'Baseline'
            for c in df.columns:
                if "Value" in c and "Baseline" not in c:
                    sales_col = c
                    break

        # 3. Net Price -> Η στήλη "Net_Price" (Καθαρή τιμή αγοράς/κόστους)
        net_cost_col = "Net_Price" if "Net_Price" in df.columns else None
        
        # 4. Sales Price -> "Sales_Without_V" (Η καθαρή λιανική που φέρνει το κέρδος)
        retail_col = "Sales_Without_V" if "Sales_Without_V" in df.columns else "Sales_Price_With_V"

        # 5. Λοιπά
        cat_col = "Segment" if "Segment" in df.columns else "Category"
        brand_col = "Brand"

        # --- ΕΠΕΞΕΡΓΑΣΙΑ ΔΕΔΟΜΕΝΩΝ ---
        df['sales_val'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
        df['net_ret'] = pd.to_numeric(df[retail_col], errors='coerce').fillna(0)
        df['net_cost'] = pd.to_numeric(df[net_cost_col], errors='coerce').fillna(0)
        
        # Υπολογισμός Margin % (GM%)
        df['gm_percent'] = 0
        mask = df['net_ret'] > 0
        df.loc[mask, 'gm_percent'] = ((df.loc[mask, 'net_ret'] - df.loc[mask, 'net_cost']) / df.loc[mask, 'net_ret']) * 100
        df['gm_percent'] = df['gm_percent'].replace([np.inf, -np.inf], 0).fillna(0)

        # ABC Analysis βασισμένη στο ΠΡΑΓΜΑΤΙΚΟ Value Sales
        df = df.sort_values('sales_val', ascending=False)
        total_sales = float(df['sales_val'].sum())
        
        if total_sales > 0:
            df['cum_perc'] = (df['sales_val'].cumsum() / total_sales) * 100
            df['abc_class'] = pd.cut(df['cum_perc'], bins=[0, 70, 90, 100.01], labels=['A', 'B', 'C'])
        else:
            df['abc_class'] = 'C'

        raw_data = []
        for _, row in df.iterrows():
            raw_data.append({
                "product_name": str(row[name_col]),
                "category": str(row[cat_col]) if cat_col in df.columns else "General",
                "brand": str(row[brand_col]) if brand_col in df.columns else "N/A",
                "sales": round(float(row['sales_val']), 2),
                "clean_sales_price": round(float(row['net_ret']), 2),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": str(row['abc_class'])
            })

        result = {
            "total_sales": round(total_sales, 2),
            "raw_data": raw_data,
            "status": "success"
        }

        supabase.table("projects").update({
            "analysis_status": "completed", 
            "analysis_json": result
        }).eq("id", project_id).execute()

        return {"status": "success"}

    except Exception as e:
        if project_id:
            supabase.table("projects").update({"analysis_status": "failed"}).eq("id", project_id).execute()
        return {"status": "error", "message": str(e)}
