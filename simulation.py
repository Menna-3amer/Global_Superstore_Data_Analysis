import pandas as pd
import time
import os
import random
from datetime import datetime

SOURCE_CSV = '/home/jovyan/work/data/superstore.csv'
STREAMING_LANDING_ZONE = "/home/jovyan/work/data_batches"

# Columns for sales data
SALES_COLS = [
    'Order.ID', 'Order.Date', 'Customer.ID', 'Customer.Name',
    'Product.ID', 'Product.Name', 'Sales', 'Quantity', 'Profit',
    'Discount', 'Shipping.Cost', 'Category', 'Sub.Category',
    'City', 'State', 'Country', 'Region', 'Segment',
    'Order.Priority', 'Ship.Mode', 'Year', 'Market'
]

os.makedirs(STREAMING_LANDING_ZONE, exist_ok=True)

def run_simulator():
    try:
        df = pd.read_csv(SOURCE_CSV)
        df.columns = df.columns.str.strip()
        
        missing = [c for c in SALES_COLS if c not in df.columns]
        if missing:
            print(f"Missing columns in CSV: {missing}")
            return

        print(f"Data Loaded. Simulating {len(SALES_COLS)} sales metrics.")
        print(f"Sending batches to {STREAMING_LANDING_ZONE}")

    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    batch_size = 200
    
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i:i+batch_size][SALES_COLS].copy()
        
        # Add extra columns (like sensor_id in TA code)
        cust_id = random.randint(1, 50)
        chunk['simulated_customer_group'] = f'CUST-GROUP-{cust_id}'
        chunk['event_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Clean numeric columns
        numeric_cols = ['Sales', 'Quantity', 'Profit', 'Discount', 'Shipping.Cost']
        for col in numeric_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0)
        
        file_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        file_path = os.path.join(STREAMING_LANDING_ZONE, f"sales_batch_{file_id}.csv")
        
        # Save as CSV (not JSON)
        chunk.to_csv(file_path, index=False, encoding='utf-8')
        
        print(f"Sent Batch {i//batch_size + 1} | Records: {len(chunk)} | Saved to: {file_path}")
        
        time.sleep(10)

if __name__== "__main__":
    try:
        run_simulator()
    except KeyboardInterrupt:
        print("Simulator Stopped by User.")
