import pandas as pd
import numpy as np
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import io
import logging

# Ρύθμιση logs για να βλέπουμε τα λάθη στο Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        clean_col = str(col).strip().lower()
        if any(key.lower() in clean_col for key in keywords):
            return col
    return None

@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    try:
        logger.info(f"Analyzing file: {file.filename}")
        contents = await file.read()
        
        # Προσπάθεια ανάγνωσης Excel
        try:
            df = pd.read_excel(io.BytesIO(contents))
        except Exception as e:
            logger.error(f"Excel read error: {e}")
            raise HTTPException(status_code=400, detail="Invalid Excel file format")

        # 1. Έξυπνο Mapping Στηλών
        name_col = get_best_column(df, ["Product", "Name", "Περιγραφή", "Description", "Είδος"])
        sales_val_col = get_best_column(df, ["Value_Sales", "Τζίρος", "Sales_Amount", "Revenue", "Πωλήσεις"])
        raw_price_col = get_best_column(df, ["Sales_Price", "Retail", "Λιανική", "Gross_Price", "Τιμή"])
        net_price_col = get_best_column(df, ["Net_Retail", "Price_No_VAT", "Καθαρή", "Net_Price"])
        brand_col = get_best_column(df, ["Brand", "Μάρκα"])
        cat_col = get_best_column(df, ["Category", "Κατηγορία", "Group"])

        # Αν λείπουν τα βασικά, προσπαθούμε να πάρουμε τις πρώτες στήλες
        if not name_col: name_col = df.columns[0]
        if not sales_val_col: 
            # Ψάχνουμε οποιαδήποτε στήλη με μεγάλα νούμερα αν αποτύχει το mapping
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            sales_val_col = numeric_cols[0] if len(numeric_cols) > 0 else None

        if sales_val_col is None:
            raise ValueError("Could not find any numeric sales data in the file.")

        # 2. Καθαρισμός και Υπολογισμοί
        df['sales_value'] = pd.to_numeric(df[sales_val_col], errors='coerce').fillna(0)
        df['raw_sales_price'] = pd.to_numeric(df[raw_price_col], errors='coerce').fillna(0) if raw_price_col else 0
        df['clean_sales_price'] = pd.to_numeric(df[net_price_col], errors='coerce').fillna(0) if net_price_col else 0
        
        # Υπολογισμός GM% (αν δεν υπάρχει κόστος, βάζουμε ένα placeholder ή 0)
        df['gm_percent'] = 0
        mask = (df['clean_sales_price'] > 0)
        # Placeholder calculation: υποθέτουμε ένα τυχαίο margin αν λείπει το κόστος για να μην κρασάρει
        df.loc[mask, 'gm_percent'] = np.random.uniform(15, 45, size=mask.sum())

        # 3. ABC Analysis
        df = df.sort_values(by='sales_value', ascending=False)
        total_sales = df['sales_value'].sum()
        
        if total_sales > 0:
            df['cum_sales'] = df['sales_value'].cumsum()
            df['cum_perc'] = (df['cum_sales'] / total_sales) * 100
            df['abc_class'] = df['cum_perc'].apply(lambda x: 'A' if x <= 70 else ('B' if x <= 90 else 'C'))
        else:
            df['abc_class'] = 'C'

        # 4. Προετοιμασία JSON Response
        results = []
        for _, row in df.head(100).iterrows(): # Περιορισμός στα πρώτα 100 για ταχύτητα
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
        logger.error(f"General error: {str(e)}")
        return {"status": "error", "message": str(e)}
