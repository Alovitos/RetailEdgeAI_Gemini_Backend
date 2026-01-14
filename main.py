import pandas as pd
import json
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "Hedginq Backend is Running"}

@app.post("/analyze")
async def analyze_data(file: UploadFile = File(...)):
    # Διαβάζουμε το αρχείο
    contents = await file.read()
    # Χρησιμοποιούμε io.BytesIO για να το διαβάσει η pandas
    df = pd.read_excel(io.BytesIO(contents))
    
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
    
    # Defaults
    required = ['value_sales', 'unit_sales', 'net_price', 'current_price']
    for col in required:
        if col not in df.columns: df[col] = 0
            
    if 'value_sales_ya' not in df.columns: df['value_sales_ya'] = None
    if 'unit_sales_ya' not in df.columns: df['unit_sales_ya'] = None

    analysis_data = []
    for _, row in df.iterrows():
        item = row.to_dict()
        
        # Growth Logic
        try:
            val_ya = float(row['value_sales_ya'])
            item['sales_growth'] = round(((float(row['value_sales']) - val_ya) / val_ya) * 100, 2) if val_ya > 0 else None
        except: item['sales_growth'] = None

        try:
            vol_ya = float(row['unit_sales_ya'])
            item['volume_growth'] = round(((float(row['unit_sales']) - vol_ya) / vol_ya) * 100, 2) if vol_ya > 0 else None
        except: item['volume_growth'] = None
        
        # Margin Logic
        try:
            m = float(row['gm_percent'])
            item['gm_percent'] = m * 100 if m < 1 else m
        except: item['gm_percent'] = 0
            
        analysis_data.append(item)

    # Category Benchmarks
    cat_benchmarks = {}
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
