import pandas as pd
import numpy as np
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_best_column(df, keywords):
    for col in df.columns:
        if any(key.lower() in str(col).lower() for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        # Smart Mapping
        name_col = get_best_column(df, ["Product", "Name", "Περιγραφή"])
        sales_val_col = get_best_column(df, ["Value_Sales", "Τζίρος", "Total"])
        raw_price_col = get_best_column(df, ["Sales_Price", "Retail", "Λιανική"])
        net_price_col = get_best_column(df, ["Net_Retail", "Net_Price", "Καθαρή"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία"])

        # Fallback αν λείπουν στήλες
        if not name_col: name_col = df.columns[0]
        if not sales_val_col: sales_val_col = df.select_dtypes(include=[np.number]).columns[0]

        # Cleanup
        df['sales_value'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['raw_sales_price'] = pd.to_numeric(df[raw_price_col], errors='coerce').fillna(0) if raw_price_col else 0
        df['clean_sales_price'] = pd.to_numeric(df[net_price_col], errors='coerce').fillna(0) if net_price_col else 0
        
        # ABC Analysis
        df = df.sort_values(by='sales_value', ascending=False)
        total_sales = float(df['sales_value'].sum())
        
        df['cum_sales'] = df['sales_value'].cumsum()
        df['cum_perc'] = (df['cum_sales'] / total_sales * 100) if total_sales > 0 else 0
        df['abc_class'] = df['cum_perc'].apply(lambda x: 'A' if x <= 70 else ('B' if x <= 90 else 'C'))

        # Προσομοίωση GM% για το demo
        df['gm_percent'] = np.random.uniform(20, 40, size=len(df))

        # Δημιουργία λίστας αποτελεσμάτων
        data_list = []
        for _, row in df.iterrows():
            data_list.append({
                "product_name": str(row[name_col]),
                "brand": str(row[brand_col]) if brand_col else "N/A",
                "category": str(row[cat_col]) if cat_col else "N/A",
                "sales": float(row['sales_value']),
                "raw_sales_price": float(row['raw_sales_price']),
                "clean_sales_price": float(row['clean_sales_price']),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": row['abc_class']
            })

        # Επιστροφή δεδομένων με τα σωστά keys για το Lovable
        return {
            "status": "success",
            "total_value": total_sales,
            "product_count": len(df),
            "data": data_list
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}
