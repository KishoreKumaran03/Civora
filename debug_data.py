#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

wb_path = Path('Testing Data/CIVORA_5Y_Retail_Data.xlsx')

# Load Sales_Data
sales = pd.read_excel(wb_path, sheet_name='Sales_Data')
sales['ds'] = pd.to_datetime(sales['ds'], errors='coerce')

# Extract just Rev and Cost and aggregate by month
sales['month'] = sales['ds'].dt.to_period('M')

monthly_rev = sales.groupby('month')['Revenue'].sum().reset_index()
monthly_rev.columns = ['month', 'revenue']

monthly_cost = sales.groupby('month')['Cost'].sum().reset_index()
monthly_cost.columns = ['month', 'cost']

print('Monthly aggregation (first 10 months):')
result = pd.merge(monthly_rev, monthly_cost, on='month')
result['profit'] = result['revenue'] - result['cost']
print(result.head(10))
print()

# Check April 2025
april_2025 = result[result['month'].astype(str).str.startswith('2025-04')]
if not april_2025.empty:
    print('April 2025 data:')
    print(april_2025)
    print('In crores: Revenue=', april_2025.iloc[0]['revenue']/10000000, 'Cr, Cost=', april_2025.iloc[0]['cost']/10000000, 'Cr')
else:
    print('No April 2025 data found')
print()

# Check all 2025 months
year_2025 = result[result['month'].astype(str).str.startswith('2025')]
print('All 2025 months:')
print(year_2025)
print()

# Check if there's data beyond 2025
print('Unique months (last 10):')
print(result['month'].tail(10).tolist())
