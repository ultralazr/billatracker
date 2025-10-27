# -*- coding: utf-8 -*-
"""Enhanced Billa Analysis for GitHub Actions - Fixed Version with 3-year plot window"""

import pandas as pd
from datetime import datetime, timedelta
import json

print("Starting Billa Price Analysis...")

# Get & sort price history file
print("\n1. Loading price history data...")
url = 'https://raw.githubusercontent.com/ultralazr/billatracker/refs/heads/main/updated_combined_dataframe.csv'
updated_combined_dataframe = pd.read_csv(url, dtype={
    'bundleInfo': str, 
    'price_regular_promotionText': str, 
    'price_regular_promotionType': str, 
    'volumeLabelShort': str, 
    'packageLabel': str, 
    'price_discountPercentage': str
})

print(f"Loaded {len(updated_combined_dataframe)} price records")

updated_combined_dataframe['date'] = pd.to_datetime(updated_combined_dataframe['date'], format='mixed')
df = updated_combined_dataframe[updated_combined_dataframe['date'].dt.date >= pd.to_datetime('2023-10-13').date()].copy()

# Fetch the latest product data
print("\n2. Loading product data...")
url = 'https://raw.githubusercontent.com/ultralazr/billatracker/refs/heads/main/flattened_product_data.csv'
flattened_product_data = pd.read_csv(url)
print(f"Loaded {len(flattened_product_data)} products")

# Create price_analysis dataframe
print("\n3. Creating price analysis dataset...")
three_months_ago = datetime.now().date() - timedelta(days=90)
one_year_ago = datetime.now().date() - timedelta(days=365)

recent_skus_df = updated_combined_dataframe[updated_combined_dataframe['date'].dt.date >= three_months_ago].copy()
skus_last_3_months = recent_skus_df['sku'].unique()

older_skus_df = updated_combined_dataframe[updated_combined_dataframe['date'].dt.date <= one_year_ago].copy()
skus_one_year_ago_or_older = older_skus_df['sku'].unique()

valid_skus = list(set(skus_last_3_months) & set(skus_one_year_ago_or_older))

flattened_product_skus = flattened_product_data['sku'].tolist()
valid_skus = [sku for sku in valid_skus if sku in flattened_product_skus]

price_analysis = pd.DataFrame({'SKU': valid_skus})
print(f"Analyzing {len(price_analysis)} products")

# Get latest prices
updated_combined_dataframe_sorted = updated_combined_dataframe.sort_values(by=['sku', 'date'])
latest_prices = updated_combined_dataframe_sorted.groupby('sku').tail(1)[['sku', 'price_regular_value']]
price_analysis = price_analysis.merge(latest_prices, left_on='SKU', right_on='sku', how='left')
price_analysis = price_analysis.rename(columns={'price_regular_value': 'price_today'}).drop('sku', axis=1)

# Create comparison reference (all available history, not just 40 months)
print("\n4. Building comparison reference...")
valid_skus = price_analysis['SKU'].tolist()
comparison_reference = updated_combined_dataframe[updated_combined_dataframe['sku'].isin(valid_skus)].copy()
comparison_reference = comparison_reference[['sku', 'date', 'price_regular_value']]
comparison_reference = comparison_reference.drop_duplicates().copy()
print(f"Comparison reference has {len(comparison_reference)} records")

# Calculate 1 year ago prices
print("\n5. Calculating 1-year price changes...")
comparison_reference_before_1yr = comparison_reference[comparison_reference['date'].dt.date <= one_year_ago].copy()
comparison_reference_before_1yr['date_diff'] = (comparison_reference_before_1yr['date'].dt.date - one_year_ago).abs()
closest_dates_indices = comparison_reference_before_1yr.groupby('sku')['date_diff'].idxmin()
closest_dates_rows = comparison_reference_before_1yr.loc[closest_dates_indices].copy()
closest_dates_1yr = closest_dates_rows[['sku', 'date']].copy()
closest_dates_1yr = closest_dates_1yr.rename(columns={'date': 'date-1yr'})

if 'date-1yr' in price_analysis.columns:
    price_analysis = price_analysis.drop('date-1yr', axis=1)
price_analysis = price_analysis.merge(closest_dates_1yr, left_on='SKU', right_on='sku', how='left')
price_analysis = price_analysis.drop('sku', axis=1)

price_at_1yr_dates = price_analysis[['SKU', 'date-1yr']].copy()
price_at_1yr_dates = price_at_1yr_dates.rename(columns={'date-1yr': 'date'})
price_at_1yr = price_at_1yr_dates.merge(comparison_reference, left_on=['SKU', 'date'], right_on=['sku', 'date'], how='left')
price_at_1yr = price_at_1yr[['SKU', 'price_regular_value']].rename(columns={'price_regular_value': 'price_1yr'})

if 'price_1yr' in price_analysis.columns:
    price_analysis = price_analysis.drop('price_1yr', axis=1)
price_analysis = price_analysis.merge(price_at_1yr, on='SKU', how='left')

price_analysis['price_change_1yr (%)'] = ((price_analysis['price_today'] - price_analysis['price_1yr']) / price_analysis['price_1yr']) * 100
price_analysis['price_change_1yr (%)'] = price_analysis['price_change_1yr (%)'].round(2)

# Calculate 2 years ago prices
print("\n6. Calculating 2-year price changes...")
two_years_ago = datetime.now().date() - timedelta(days=2 * 365.25)
comparison_reference_before_2yr = comparison_reference[comparison_reference['date'].dt.date <= two_years_ago].copy()
comparison_reference_before_2yr['date_diff'] = (comparison_reference_before_2yr['date'].dt.date - two_years_ago).abs()
closest_dates_indices_2yr = comparison_reference_before_2yr.groupby('sku')['date_diff'].idxmin()
closest_dates_rows_2yr = comparison_reference_before_2yr.loc[closest_dates_indices_2yr].copy()
closest_data_2yr = closest_dates_rows_2yr[['sku', 'date', 'price_regular_value']].copy()
closest_data_2yr = closest_data_2yr.rename(columns={'date': 'date-2yr', 'price_regular_value': 'price_2yr_ago'})

if 'date-2yr' in price_analysis.columns:
    price_analysis = price_analysis.drop('date-2yr', axis=1)
if 'price_2yr_ago' in price_analysis.columns:
    price_analysis = price_analysis.drop('price_2yr_ago', axis=1)
price_analysis = price_analysis.merge(closest_data_2yr, left_on='SKU', right_on='sku', how='left').drop('sku', axis=1)

price_analysis['price_change_2yr (%)'] = ((price_analysis['price_today'] - price_analysis['price_2yr_ago']) / price_analysis['price_2yr_ago']) * 100
price_analysis['price_change_2yr (%)'] = price_analysis['price_change_2yr (%)'].round(2)

# Calculate 3 years ago prices
print("\n7. Calculating 3-year price changes...")
three_years_ago = datetime.now().date() - timedelta(days=3 * 365.25)
comparison_reference_before_3yr = comparison_reference[comparison_reference['date'].dt.date <= three_years_ago].copy()
comparison_reference_before_3yr['date_diff'] = (comparison_reference_before_3yr['date'].dt.date - three_years_ago).abs()
closest_dates_indices_3yr = comparison_reference_before_3yr.groupby('sku')['date_diff'].idxmin()
closest_dates_rows_3yr = comparison_reference_before_3yr.loc[closest_dates_indices_3yr].copy()
closest_data_3yr = closest_dates_rows_3yr[['sku', 'date', 'price_regular_value']].copy()
closest_data_3yr = closest_data_3yr.rename(columns={'date': 'date-3yr', 'price_regular_value': 'price_3yr_ago'})

if 'date-3yr' in price_analysis.columns:
    price_analysis = price_analysis.drop('date-3yr', axis=1)
if 'price_3yr_ago' in price_analysis.columns:
    price_analysis = price_analysis.drop('price_3yr_ago', axis=1)
price_analysis = price_analysis.merge(closest_data_3yr, left_on='SKU', right_on='sku', how='left').drop('sku', axis=1)

price_analysis['price_change_3yr (%)'] = ((price_analysis['price_today'] - price_analysis['price_3yr_ago']) / price_analysis['price_3yr_ago']) * 100
price_analysis['price_change_3yr (%)'] = price_analysis['price_change_3yr (%)'].round(2)

# Filter out products with zero or null historical prices before selecting top 10
print("\n8. Identifying top 10 products for each time period (excluding zero prices)...")
valid_1yr = price_analysis[(price_analysis['price_1yr'] > 0) & (price_analysis['price_1yr'].notna())]
valid_2yr = price_analysis[(price_analysis['price_2yr_ago'] > 0) & (price_analysis['price_2yr_ago'].notna())]
valid_3yr = price_analysis[(price_analysis['price_3yr_ago'] > 0) & (price_analysis['price_3yr_ago'].notna())]

top_10_1yr = valid_1yr.nlargest(10, 'price_change_1yr (%)')
top_10_2yr = valid_2yr.nlargest(10, 'price_change_2yr (%)')
top_10_3yr = valid_3yr.nlargest(10, 'price_change_3yr (%)')

print("\nTop 10 products with biggest 1-year price increases:")
print(top_10_1yr[['SKU', 'price_1yr', 'price_today', 'price_change_1yr (%)']].to_string())

print("\nTop 10 products with biggest 2-year price increases:")
print(top_10_2yr[['SKU', 'price_2yr_ago', 'price_today', 'price_change_2yr (%)']].to_string())

print("\nTop 10 products with biggest 3-year price increases:")
print(top_10_3yr[['SKU', 'price_3yr_ago', 'price_today', 'price_change_3yr (%)']].to_string())

# Combine all unique SKUs from top 10 lists
all_top_skus = list(set(top_10_1yr['SKU'].tolist() + top_10_2yr['SKU'].tolist() + top_10_3yr['SKU'].tolist()))

# Get product names for all top SKUs
print("\n9. Getting product names...")
top_products_with_names = price_analysis[price_analysis['SKU'].isin(all_top_skus)].merge(
    flattened_product_data[['sku', 'name']], 
    left_on='SKU', 
    right_on='sku', 
    how='left'
).drop('sku', axis=1)

# Get complete price history for all top products - LIMITED TO LAST 3 YEARS
print("\n10. Extracting price history (last 3 years only)...")
three_years_ago_cutoff = datetime.now().date() - timedelta(days=3 * 365.25)
price_history_all = comparison_reference[
    (comparison_reference['sku'].isin(all_top_skus)) & 
    (comparison_reference['date'].dt.date >= three_years_ago_cutoff)
].copy()
price_history_all = price_history_all.sort_values(['sku', 'date'])

# Add product names to price history
price_history_all = price_history_all.merge(
    flattened_product_data[['sku', 'name']], 
    on='sku', 
    how='left'
)

print(f"Extracted {len(price_history_all)} price history records (last 3 years)")

# Prepare data structure for visualization
print("\n11. Preparing data for visualization...")
chart_data = {
    '1yr': {},
    '2yr': {},
    '3yr': {}
}

# Process 1-year top 10
for _, row in top_10_1yr.iterrows():
    sku = row['SKU']
    sku_data = price_history_all[price_history_all['sku'] == sku].copy()
    product_name = sku_data['name'].iloc[0] if len(sku_data) > 0 else f"SKU {sku}"
    
    chart_data['1yr'][sku] = {
        'name': product_name,
        'sku': sku,
        'price_today': float(row['price_today']),
        'price_1yr': float(row['price_1yr']) if pd.notna(row['price_1yr']) else None,
        'price_2yr': float(row['price_2yr_ago']) if pd.notna(row['price_2yr_ago']) else None,
        'price_3yr': float(row['price_3yr_ago']) if pd.notna(row['price_3yr_ago']) else None,
        'change_1yr': float(row['price_change_1yr (%)']) if pd.notna(row['price_change_1yr (%)']) else None,
        'change_2yr': float(row['price_change_2yr (%)']) if pd.notna(row['price_change_2yr (%)']) else None,
        'change_3yr': float(row['price_change_3yr (%)']) if pd.notna(row['price_change_3yr (%)']) else None,
        'dates': sku_data['date'].dt.strftime('%Y-%m-%d').tolist(),
        'prices': sku_data['price_regular_value'].tolist()
    }

# Process 2-year top 10
for _, row in top_10_2yr.iterrows():
    sku = row['SKU']
    sku_data = price_history_all[price_history_all['sku'] == sku].copy()
    product_name = sku_data['name'].iloc[0] if len(sku_data) > 0 else f"SKU {sku}"
    
    chart_data['2yr'][sku] = {
        'name': product_name,
        'sku': sku,
        'price_today': float(row['price_today']),
        'price_1yr': float(row['price_1yr']) if pd.notna(row['price_1yr']) else None,
        'price_2yr': float(row['price_2yr_ago']) if pd.notna(row['price_2yr_ago']) else None,
        'price_3yr': float(row['price_3yr_ago']) if pd.notna(row['price_3yr_ago']) else None,
        'change_1yr': float(row['price_change_1yr (%)']) if pd.notna(row['price_change_1yr (%)']) else None,
        'change_2yr': float(row['price_change_2yr (%)']) if pd.notna(row['price_change_2yr (%)']) else None,
        'change_3yr': float(row['price_change_3yr (%)']) if pd.notna(row['price_change_3yr (%)']) else None,
        'dates': sku_data['date'].dt.strftime('%Y-%m-%d').tolist(),
        'prices': sku_data['price_regular_value'].tolist()
    }

# Process 3-year top 10
for _, row in top_10_3yr.iterrows():
    sku = row['SKU']
    sku_data = price_history_all[price_history_all['sku'] == sku].copy()
    product_name = sku_data['name'].iloc[0] if len(sku_data) > 0 else f"SKU {sku}"
    
    chart_data['3yr'][sku] = {
        'name': product_name,
        'sku': sku,
        'price_today': float(row['price_today']),
        'price_1yr': float(row['price_1yr']) if pd.notna(row['price_1yr']) else None,
        'price_2yr': float(row['price_2yr_ago']) if pd.notna(row['price_2yr_ago']) else None,
        'price_3yr': float(row['price_3yr_ago']) if pd.notna(row['price_3yr_ago']) else None,
        'change_1yr': float(row['price_change_1yr (%)']) if pd.notna(row['price_change_1yr (%)']) else None,
        'change_2yr': float(row['price_change_2yr (%)']) if pd.notna(row['price_change_2yr (%)']) else None,
        'change_3yr': float(row['price_change_3yr (%)']) if pd.notna(row['price_change_3yr (%)']) else None,
        'dates': sku_data['date'].dt.strftime('%Y-%m-%d').tolist(),
        'prices': sku_data['price_regular_value'].tolist()
    }

# Create HTML visualization
print("\n12. Generating interactive HTML report...")
html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Billa Price Analysis - Top 10 Price Increases</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@2.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
            padding: 20px;
        }}
        
        h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        
        .subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
            margin-bottom: 20px;
        }}
        
        .update-time {{
            font-size: 0.9em;
            opacity: 0.8;
        }}
        
        .tabs {{
            display: flex;
            justify-content: center;
            gap: 10px;
            margin-bottom: 30px;
        }}
        
        .tab {{
            background: rgba(255, 255, 255, 0.2);
            color: white;
            border: 2px solid rgba(255, 255, 255, 0.3);
            padding: 12px 30px;
            border-radius: 25px;
            cursor: pointer;
            font-size: 1em;
            font-weight: 600;
            transition: all 0.3s ease;
            backdrop-filter: blur(10px);
        }}
        
        .tab:hover {{
            background: rgba(255, 255, 255, 0.3);
            transform: translateY(-2px);
        }}
        
        .tab.active {{
            background: white;
            color: #667eea;
            border-color: white;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 30px;
            margin-bottom: 40px;
        }}
        
        .chart-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s ease;
        }}
        
        .chart-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.3);
        }}
        
        .chart-header {{
            margin-bottom: 20px;
        }}
        
        .product-name {{
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
        }}
        
        .sku-label {{
            font-size: 0.9em;
            color: #666;
            margin-bottom: 15px;
        }}
        
        .price-stats {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
            margin-top: 15px;
            padding-top: 15px;
            border-top: 2px solid #f0f0f0;
        }}
        
        .stat {{
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
        }}
        
        .stat-label {{
            font-size: 0.8em;
            color: #888;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}
        
        .stat-value {{
            font-size: 1.2em;
            font-weight: bold;
        }}
        
        .stat-row {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }}
        
        .stat-change {{
            font-size: 0.9em;
            font-weight: 600;
            margin-left: 10px;
        }}
        
        .price-increase {{
            color: #e74c3c;
        }}
        
        .price-decrease {{
            color: #27ae60;
        }}
        
        .price-current {{
            color: #27ae60;
        }}
        
        .price-old {{
            color: #3498db;
        }}
        
        canvas {{
            max-height: 300px;
        }}
        
        .view {{
            display: none;
        }}
        
        .view.active {{
            display: block;
        }}
        
        @media (max-width: 768px) {{
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            
            h1 {{
                font-size: 1.8em;
            }}
            
            .tabs {{
                flex-direction: column;
                align-items: center;
            }}
            
            .tab {{
                width: 80%;
            }}
            
            .price-stats {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Die Teuerung trifft – BILLA hilft</h1>
            <div class="subtitle">Top 10 Products with Biggest Price Increases (Last 3 Years)</div>
            <div class="update-time">Last updated: {datetime.now().strftime('%B %d, %Y at %H:%M')}</div>
        </header>
        
        <div class="tabs">
            <button class="tab active" onclick="switchView('1yr')">Top 10 over 1 Year</button>
            <button class="tab" onclick="switchView('2yr')">Top 10 over 2 Years</button>
            <button class="tab" onclick="switchView('3yr')">Top 10 over 3 Years</button>
        </div>
        
        <div id="view-1yr" class="view active">
            <div class="charts-grid" id="chartsGrid1yr"></div>
        </div>
        
        <div id="view-2yr" class="view">
            <div class="charts-grid" id="chartsGrid2yr"></div>
        </div>
        
        <div id="view-3yr" class="view">
            <div class="charts-grid" id="chartsGrid3yr"></div>
        </div>
    </div>
    
    <script>
        const chartData = {json.dumps(chart_data, indent=2)};
        const charts = {{}};
        
        function switchView(period) {{
            // Update tabs
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            event.target.classList.add('active');
            
            // Update views
            document.querySelectorAll('.view').forEach(view => view.classList.remove('active'));
            document.getElementById(`view-${{period}}`).classList.add('active');
        }}
        
        function createCharts(period, gridId) {{
            const grid = document.getElementById(gridId);
            const data = chartData[period];
            
            Object.keys(data).forEach((sku, index) => {{
                const product = data[sku];
                
                // Create card
                const card = document.createElement('div');
                card.className = 'chart-card';
                
                // Create header
                const header = document.createElement('div');
                header.className = 'chart-header';
                header.innerHTML = `
                    <div class="product-name">${{product.name}}</div>
                    <div class="sku-label">SKU: ${{product.sku}}</div>
                `;
                card.appendChild(header);
                
                // Create canvas for chart
                const canvas = document.createElement('canvas');
                canvas.id = `chart-${{period}}-${{sku}}`;
                card.appendChild(canvas);
                
                // Create stats with compact layout
                const stats = document.createElement('div');
                stats.className = 'price-stats';
                
                // Current Price (always shown)
                let statsHTML = `
                    <div class="stat">
                        <div class="stat-label">Current Price</div>
                        <div class="stat-value price-current">€${{product.price_today.toFixed(2)}}</div>
                    </div>
                `;
                
                // 1 Year Ago (price + change in same tile)
                if (product.price_1yr !== null && product.change_1yr !== null) {{
                    const changeClass = product.change_1yr >= 0 ? 'price-increase' : 'price-decrease';
                    const changeText = product.change_1yr >= 0 ? `+${{product.change_1yr.toFixed(2)}}%` : `${{product.change_1yr.toFixed(2)}}%`;
                    statsHTML += `
                        <div class="stat">
                            <div class="stat-label">1 Year Ago</div>
                            <div class="stat-row">
                                <span class="stat-value price-old">€${{product.price_1yr.toFixed(2)}}</span>
                                <span class="stat-change ${{changeClass}}">${{changeText}}</span>
                            </div>
                        </div>
                    `;
                }}
                
                // 2 Years Ago (price + change in same tile)
                if (product.price_2yr !== null && product.change_2yr !== null) {{
                    const changeClass = product.change_2yr >= 0 ? 'price-increase' : 'price-decrease';
                    const changeText = product.change_2yr >= 0 ? `+${{product.change_2yr.toFixed(2)}}%` : `${{product.change_2yr.toFixed(2)}}%`;
                    statsHTML += `
                        <div class="stat">
                            <div class="stat-label">2 Years Ago</div>
                            <div class="stat-row">
                                <span class="stat-value price-old">€${{product.price_2yr.toFixed(2)}}</span>
                                <span class="stat-change ${{changeClass}}">${{changeText}}</span>
                            </div>
                        </div>
                    `;
                }}
                
                // 3 Years Ago (price + change in same tile)
                if (product.price_3yr !== null && product.change_3yr !== null) {{
                    const changeClass = product.change_3yr >= 0 ? 'price-increase' : 'price-decrease';
                    const changeText = product.change_3yr >= 0 ? `+${{product.change_3yr.toFixed(2)}}%` : `${{product.change_3yr.toFixed(2)}}%`;
                    statsHTML += `
                        <div class="stat">
                            <div class="stat-label">3 Years Ago</div>
                            <div class="stat-row">
                                <span class="stat-value price-old">€${{product.price_3yr.toFixed(2)}}</span>
                                <span class="stat-change ${{changeClass}}">${{changeText}}</span>
                            </div>
                        </div>
                    `;
                }}
                
                stats.innerHTML = statsHTML;
                card.appendChild(stats);
                
                grid.appendChild(card);
                
                // Create chart with step plot
                const ctx = canvas.getContext('2d');
                charts[`${{period}}-${{sku}}`] = new Chart(ctx, {{
                    type: 'line',
                    data: {{
                        labels: product.dates,
                        datasets: [{{
                            label: 'Price (€)',
                            data: product.prices,
                            borderColor: index % 2 === 0 ? '#667eea' : '#e74c3c',
                            backgroundColor: index % 2 === 0 ? 'rgba(102, 126, 234, 0.1)' : 'rgba(231, 76, 60, 0.1)',
                            borderWidth: 3,
                            stepped: true,
                            fill: true,
                            pointRadius: 2,
                            pointHoverRadius: 6,
                            pointBackgroundColor: '#fff',
                            pointBorderWidth: 2
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: true,
                        plugins: {{
                            legend: {{
                                display: false
                            }},
                            tooltip: {{
                                mode: 'index',
                                intersect: false,
                                callbacks: {{
                                    label: function(context) {{
                                        return 'Price: €' + context.parsed.y.toFixed(2);
                                    }}
                                }}
                            }}
                        }},
                        scales: {{
                            x: {{
                                display: true,
                                title: {{
                                    display: true,
                                    text: 'Date'
                                }},
                                ticks: {{
                                    maxTicksLimit: 10,
                                    maxRotation: 45,
                                    minRotation: 45
                                }}
                            }},
                            y: {{
                                display: true,
                                title: {{
                                    display: true,
                                    text: 'Price (€)'
                                }},
                                ticks: {{
                                    callback: function(value) {{
                                        return '€' + value.toFixed(2);
                                    }}
                                }}
                            }}
                        }},
                        interaction: {{
                            mode: 'nearest',
                            axis: 'x',
                            intersect: false
                        }}
                    }}
                }});
            }});
        }}
        
        // Initialize all charts
        createCharts('1yr', 'chartsGrid1yr');
        createCharts('2yr', 'chartsGrid2yr');
        createCharts('3yr', 'chartsGrid3yr');
    </script>
</body>
</html>"""

# Save top 10 lists to CSV
top_10_1yr.to_csv('top_10_1yr_increases.csv', index=False)
top_10_2yr.to_csv('top_10_2yr_increases.csv', index=False)
top_10_3yr.to_csv('top_10_3yr_increases.csv', index=False)
print("✅ Top 10 lists saved to CSV files")

print("\n✅ Analysis complete!")
print(f"Analyzed {len(price_analysis)} products")
print(f"Generated report for top 10 products across 1, 2, and 3 year periods")
print(f"Charts showing price history limited to last 3 years")
print(f"Report date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Save HTML file
with open('price_analysis_report.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("✅ HTML report saved as 'price_analysis_report.html'")

# Save price_analysis to CSV
price_analysis.to_csv('price_analysis.csv', index=False)
print("✅ Price analysis saved as 'price_analysis.csv'")
