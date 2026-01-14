import pandas as pd
import json
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import io

app = FastAPI()

# Πλήρης άδεια CORS για να επικοινωνεί το Lovable χωρίς εμπόδια
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "Hedginq Backend is Online"}

@app.post("/analyze")
async def analyze_data(file: UploadFile = File(...)):
    # Ανάγνωση αρχείου
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))
    
    # Mapping στυλών - Ενοποίηση παλιών και νέων (YA)
    column_mapping = {
        'Product Name': 'product_name',
        'Category': 'category',
        'Brand': 'brand',
        'Value Sales': 'value_sales',
        'Unit Sales': 'unit_sales',
        'Net Price': 'net_price',
        'Current Price': 'current_price',
        'GM %': 'gm_percent',
        'ABC Class': 'abc_class',
        'Elasticity': 'elasticity',
        'Value Sales YA': 'value_sales_ya',
        'Unit Sales YA': 'unit_sales_ya'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Διασφάλιση βασικών στηλών για να μη "σκάσει" το render
    required = ['value_sales', 'unit_sales', 'net_price', 'current_price']
    for col in required:
        if col not in df.columns:
            df[col] = 0
            
    # Προετοιμασία λίστας αποτελεσμάτων
    analysis_data = []
    
    for _, row in df.iterrows():
        item = row.to_dict()
        
        # Υπολογισμός Sales Growth % (μόνο αν υπάρχει η στήλη και έχει τιμή)
        item['sales_growth'] = None
        if 'value_sales_ya' in row and pd.notnull(row['value_sales_ya']) and float(row['value_sales_ya']) > 0:
            item['sales_growth'] = round(((float(row['value_sales']) - float(row['value_sales_ya'])) / float(row['value_sales_ya'])) * 100, 2)

        # Υπολογισμός Volume Growth %
        item['volume_growth'] = None
        if 'unit_sales_ya' in row and pd.notnull(row['unit_sales_ya']) and float(row['unit_sales_ya']) > 0:
            item['volume_growth'] = round(((float(row['unit_sales']) - float(row['unit_sales_ya'])) / float(row['unit_sales_ya'])) * 100, 2)
        
        # Κανονικοποίηση Margin (μετατροπή 0.2 σε 20)
        try:
            m = float(row['gm_percent'])
            item['gm_percent'] = m * 100 if m < 1 else m
        except:
            item['gm_percent'] = 0
            
        analysis_data.append(item)

    # Υπολογισμός Category Benchmarks για το Negotiation Hub
    cat_benchmarks = {}
    if 'category' in df.columns:
        for cat in df['category'].unique():
            cat_df = df[df['category'] == cat]
            rev = cat_df['value_sales'].sum()
            cost = (cat_df['unit_sales'] * cat_df['net_price']).sum()
            cat_benchmarks[str(cat)] = round(((rev - cost) / rev) * 100, 2) if rev > 0 else 0

    return {
        "products": analysis_data,
        "category_benchmarks": cat_benchmarks,
        "summary": {
            "total_sales": float(df['value_sales'].sum()),
            "total_units": int(df['unit_sales'].sum())
        }
    }
