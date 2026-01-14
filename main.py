import pandas as pd
import json
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Any
import io

# ΑΠΑΡΑΙΤΗΤΟ ΓΙΑ ΤΟ RENDER
app = FastAPI()

# Επιτρέπουμε στο Lovable να μιλάει με το Backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "Hedginq Backend is Running"}

@app.post("/process")
async def process_excel(file: UploadFile = File(...)):
    # Διάβασμα του αρχείου από τη μνήμη
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))
    
    # Mapping στυλών - Με τις νέες YA στήλες
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
    
    # Fill defaults για να μη σκάει τίποτα
    required_columns = ['value_sales', 'unit_sales', 'net_price', 'current_price']
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0
            
    if 'value_sales_ya' not in df.columns:
        df['value_sales_ya'] = None
    if 'unit_sales_ya' not in df.columns:
        df['unit_sales_ya'] = None

    def calculate_growth(current, ya):
        try:
            if ya and float(ya) > 0:
                return round(((float(current) - float(ya)) / float(ya)) * 100, 2)
        except:
            pass
        return None

    analysis_data = []
    for _, row in df.iterrows():
        item = row.to_dict()
        
        # Growth calculations
        item['sales_growth'] = calculate_growth(row['value_sales'], row['value_sales_ya'])
        item['volume_growth'] = calculate_growth(row['unit_sales'], row['unit_sales_ya'])
        
        # Margin normalization
        try:
            val = float(row['gm_percent'])
            item['gm_percent'] = val * 100 if val < 1 else val
        except:
            item['gm_percent'] = 0
            
        analysis_data.append(item)

    # Category Benchmarks για το Negotiation Hub
    cat_benchmarks = {}
    for cat in df['category'].unique():
        cat_df = df[df['category'] == cat]
        total_rev = cat_df['value_sales'].sum()
        total_cost = (cat_df['unit_sales'] * cat_df['net_price']).sum()
        
        if total_rev > 0:
            avg_margin = ((total_rev - total_cost) / total_rev) * 100
            cat_benchmarks[str(cat)] = round(float(avg_margin), 2)

    return {
        "products": analysis_data,
        "category_benchmarks": cat_benchmarks,
        "summary": {
            "total_sales": float(df['value_sales'].sum()),
            "total_units": int(df['unit_sales'].sum())
        }
    }
