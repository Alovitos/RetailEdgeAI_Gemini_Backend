import pandas as pd
import json
from typing import Dict, List, Any

def process_analysis_data(file_path: str) -> str:
    # Φόρτωση του Excel
    df = pd.read_excel(file_path)
    
    # Mapping στυλών - Προσθήκη των YA (Year Ago) με ασφάλεια
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
        # Νέες στήλες Year Ago
        'Value Sales YA': 'value_sales_ya',
        'Unit Sales YA': 'unit_sales_ya'
    }
    
    # Μετονομασία βάσει του mapping (μόνο όσες στήλες υπάρχουν)
    df = df.rename(columns=column_mapping)
    
    # Διασφάλιση ότι υπάρχουν οι απαραίτητες στήλες για τους υπάρχοντες υπολογισμούς
    required_columns = ['value_sales', 'unit_sales', 'net_price', 'current_price']
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0
            
    # Χειρισμός των YA στηλών αν λείπουν (για να μη σπάσει ο κώδικας)
    if 'value_sales_ya' not in df.columns:
        df['value_sales_ya'] = None
    if 'unit_sales_ya' not in df.columns:
        df['unit_sales_ya'] = None

    # Υπολογισμός Growth Metrics (μόνο αν υπάρχουν δεδομένα YA)
    def calculate_growth(current, ya):
        if ya and ya > 0:
            return round(((current - ya) / ya) * 100, 2)
        return None

    # Προσθήκη επιπλέον υπολογισμένων πεδίων για το Frontend
    analysis_data = []
    for _, row in df.iterrows():
        item = row.to_dict()
        
        # Υπολογισμός Growth για το Dashboard
        item['sales_growth'] = calculate_growth(row['value_sales'], row['value_sales_ya'])
        item['volume_growth'] = calculate_growth(row['unit_sales'], row['unit_sales_ya'])
        
        # Διατήρηση της λογικής για το Negotiation & Goal Seeker
        # Εξασφαλίζουμε ότι το gm_percent είναι αριθμός
        try:
            item['gm_percent'] = float(row['gm_percent']) * 100 if row['gm_percent'] < 1 else float(row['gm_percent'])
        except:
            item['gm_percent'] = 0
            
        analysis_data.append(item)

    # Υπολογισμός Category Averages για το Negotiation Tool
    cat_averages = df.groupby('category').agg({
        'value_sales': 'sum',
        'net_price': 'mean',
        'current_price': 'mean'
    }).reset_index()
    
    # Υπολογισμός Weighted Category Margin
    # (Total Sales - Total Cost) / Total Sales
    # Εδώ χτίζουμε το benchmark που ζήτησες για το Negotiation Hub
    cat_benchmarks = {}
    for cat in df['category'].unique():
        cat_df = df[df['category'] == cat]
        total_rev = cat_df['value_sales'].sum()
        # Υπολογιστικό κόστος = unit_sales * net_price
        total_cost = (cat_df['unit_sales'] * cat_df['net_price']).sum()
        
        if total_rev > 0:
            avg_margin = ((total_rev - total_cost) / total_rev) * 100
        else:
            avg_margin = 0
            
        cat_benchmarks[cat] = round(avg_margin, 2)

    # Τελικό Output
    output = {
        "products": analysis_data,
        "category_benchmarks": cat_benchmarks,
        "summary": {
            "total_sales": float(df['value_sales'].sum()),
            "total_units": int(df['unit_sales'].sum()),
            "avg_margin": float(df['gm_percent'].mean()) if 'gm_percent' in df.columns else 0
        }
    }
    
    return json.dumps(output, indent=4)
