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
    """Ανιχνεύει την καταλληλότερη στήλη βάσει λέξεων-κλειδιών."""
    for col in df.columns:
        if any(key.lower() in col.lower() for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        # 1. Έξυπνο Mapping Στηλών
        name_col = get_best_column(df, ["Product", "Name", "Περιγραφή", "Description"])
        sales_val_col = get_best_column(df, ["Value_Sales", "Τζίρος", "Sales_Amount", "Revenue"])
        raw_price_col = get_best_column(df, ["Sales_Price", "Retail", "Λιανική", "Gross_Price"]) # Τιμή με ΦΠΑ
        net_price_col = get_best_column(df, ["Net_Retail", "Price_No_VAT", "Καθαρή", "Net_Price"]) # Τιμή προ ΦΠΑ
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group"])

        # Έλεγχος mandatory στηλών
        if not all([name_col, sales_val_col]):
            raise HTTPException(status_code=400, detail="Missing mandatory columns (Name or Sales Value)")

        # 2. Καθαρισμός και Υπολογισμοί
        df['sales_value'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['raw_sales_price'] = pd.to_numeric(df[raw_price_col], errors='coerce').fillna(0) if raw_price_col else 0
        df['clean_sales_price'] = pd.to_numeric(df[net_price_col], errors='coerce').fillna(0) if net_price_col else 0
        
        # Υπολογισμός GM% αν υπάρχουν οι τιμές
        df['gm_percent'] = 0
        mask = (df['clean_sales_price'] > 0) # Χρήση της καθαρής τιμής για το margin
        # Εδώ υποθέτουμε ότι το κόστος θα μπορούσε να είναι μια άλλη στήλη, 
        # για τώρα κρατάμε τη δομή που ζήτησες
        df.loc[mask, 'gm_percent'] = ((df['clean_sales_price'] - (df['clean_sales_price'] * 0.7)) / df['clean_sales_price']) * 100

        # 3. ABC Analysis
        df = df.sort_values(by='sales_value', ascending=False)
        df['cum_sales'] = df['sales_value'].cumsum()
        total_sales = df['sales_value'].sum()
        df['cum_perc'] = (df['cum_sales'] / total_sales) * 100

        def classify_abc(perc):
            if perc <= 70: return 'A'
            if perc <= 90: return 'B'
            return 'C'

        df['abc_class'] = df['cum_perc'].apply(classify_abc)

        # 4. Προετοιμασία JSON Response
        results = []
        for _, row in df.iterrows():
            results.append({
                "product_name": str(row[name_col]),
                "brand": str(row[brand_col]) if brand_col else "N/A",
                "category": str(row[cat_col]) if cat_col else "N/A",
                "sales": float(row['sales_value']),
                "raw_sales_price": float(row['raw_sales_price']),
                "clean_sales_price": float(row['clean_sales_price']),
                "gm_percent": round(float(row['gm_percent']), 2),
                "abc_class": row['abc_class']
            })

        return {
            "status": "success",
            "total_value": float(total_sales),
            "product_count": len(df),
            "data": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
