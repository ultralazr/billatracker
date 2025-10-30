# -*- coding: utf-8 -*-
"""
Cheapflation Analysis - GitHub Actions Version
Generates HTML report with price change analysis for Nahrungsmittel products
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for GitHub Actions
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

# Create output directory (will replace existing files)
OUTPUT_DIR = 'output'
if os.path.exists(OUTPUT_DIR):
    # Clean up old files
    for file in os.listdir(OUTPUT_DIR):
        file_path = os.path.join(OUTPUT_DIR, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
else:
    os.makedirs(OUTPUT_DIR)

# Dictionary to store all statistics for HTML generation
stats = {
    '1yr': {},
    '2yr': {},
    '3yr': {}
}

# Load the data
print("Loading data...")
url_product_data = 'https://raw.githubusercontent.com/ultralazr/billatracker/refs/heads/main/flattened_product_data.csv'
flattened_product_data = pd.read_csv(url_product_data)

url_price_analysis = 'https://raw.githubusercontent.com/ultralazr/billatracker/refs/heads/main/price_analysis.csv'
price_analysis = pd.read_csv(url_price_analysis)

# Filter for 'Nahrungsmittel' (case-insensitive) and select sku and name
nahrungsmittel = flattened_product_data[flattened_product_data['parentCategories'].str.contains('Nahrungsmittel', na=False, case=False)][['sku', 'name']]

stats['total_products'] = len(nahrungsmittel)
print(f"Total number of products in Nahrungsmittel: {stats['total_products']}")

# Filter and merge data for each category and time period
clever_nahrungsmittel = nahrungsmittel[nahrungsmittel['name'].str.lower().str.startswith('clever', na=False)]
merged_clever_1yr_df = pd.merge(clever_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_1yr (%)'])
merged_clever_2yr_df = pd.merge(clever_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_2yr (%)'])
merged_clever_3yr_df = pd.merge(clever_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_3yr (%)'])

billa_nahrungsmittel = nahrungsmittel[nahrungsmittel['name'].str.lower().str.contains('billa', na=False)]
merged_billa_1yr_df = pd.merge(billa_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_1yr (%)'])
merged_billa_2yr_df = pd.merge(billa_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_2yr (%)'])
merged_billa_3yr_df = pd.merge(billa_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_3yr (%)'])

ja_nahrungsmittel = nahrungsmittel[nahrungsmittel['name'].str.lower().str.startswith('ja!', na=False)]
merged_ja_1yr_df = pd.merge(ja_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_1yr (%)'])
merged_ja_2yr_df = pd.merge(ja_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_2yr (%)'])
merged_ja_3yr_df = pd.merge(ja_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_3yr (%)'])

other_brands_nahrungsmittel = nahrungsmittel[~nahrungsmittel['name'].str.lower().str.startswith('clever', na=False) & ~nahrungsmittel['name'].str.lower().str.contains('billa', na=False) & ~nahrungsmittel['name'].str.lower().str.startswith('ja!', na=False)]
merged_other_brands_1yr_df = pd.merge(other_brands_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_1yr (%)'])
merged_other_brands_2yr_df = pd.merge(other_brands_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_2yr (%)'])
merged_other_brands_3yr_df = pd.merge(other_brands_nahrungsmittel, price_analysis, left_on='sku', right_on='SKU', how='inner').dropna(subset=['price_change_3yr (%)'])

# Store product counts
stats['1yr']['total'] = len(merged_clever_1yr_df) + len(merged_billa_1yr_df) + len(merged_ja_1yr_df) + len(merged_other_brands_1yr_df)
stats['2yr']['total'] = len(merged_clever_2yr_df) + len(merged_billa_2yr_df) + len(merged_ja_2yr_df) + len(merged_other_brands_2yr_df)
stats['3yr']['total'] = len(merged_clever_3yr_df) + len(merged_billa_3yr_df) + len(merged_ja_3yr_df) + len(merged_other_brands_3yr_df)

print(f"Number of products with 1-year price change data: {stats['1yr']['total']}")
print(f"Number of products with 2-year price change data: {stats['2yr']['total']}")
print(f"Number of products with 3-year price change data: {stats['3yr']['total']}")

# Function to calculate optimal bins and range for histograms
def calculate_bin_params(data, range_percentiles=(1, 99)):
    """
    Calculates optimal number of bins using Freedman-Diaconis rule
    and determines range based on specified percentiles.
    Ensures a minimum number of bins to cover a 5% range.
    """
    q1 = np.percentile(data, range_percentiles[0])
    q3 = np.percentile(data, range_percentiles[1])
    iqr = q3 - q1
    bin_width = 2 * iqr / (len(data) ** (1/3))
    dynamic_bins = int(np.ceil((data.max() - data.min()) / bin_width))
    min_bins_5percent = int(np.ceil((q3 - q1) / 5))
    bins = max(dynamic_bins, min_bins_5percent)
    return bins, (q1, q3)

# Function to process and generate plots for a time period
def process_time_period(period, period_label, 
                       clever_df, billa_df, ja_df, other_df):
    """
    Process data and generate all plots for a given time period
    """
    print(f"\n--- {period_label} Price Change Analysis ---")
    
    price_col = f'price_change_{period} (%)'
    
    # Combine all data for bin calculation
    all_data = pd.concat([clever_df[price_col], billa_df[price_col],
                         ja_df[price_col], other_df[price_col]])
    
    bins, range_vals = calculate_bin_params(all_data)
    bin_edges = np.arange(int(range_vals[0]/5)*5 - 2.5, int(range_vals[1]/5)*5 + 1 + 2.5, 5)
    
    # Store stats
    stats[period]['bins'] = len(bin_edges) - 1
    stats[period]['range_min'] = range_vals[0]
    stats[period]['range_max'] = range_vals[1]
    
    # Calculate histograms and percentages
    clever_hist, _ = np.histogram(clever_df[price_col], bins=bin_edges)
    clever_percentages = (clever_hist / clever_hist.sum()) * 100
    
    billa_hist, _ = np.histogram(billa_df[price_col], bins=bin_edges)
    billa_percentages = (billa_hist / billa_hist.sum()) * 100
    
    ja_hist, _ = np.histogram(ja_df[price_col], bins=bin_edges)
    ja_percentages = (ja_hist / ja_hist.sum()) * 100
    
    other_hist, _ = np.histogram(other_df[price_col], bins=bin_edges)
    other_percentages = (other_hist / other_hist.sum()) * 100
    
    max_percentage = max(clever_percentages.max(), billa_percentages.max(), 
                        ja_percentages.max(), other_percentages.max())
    y_limit = max_percentage * 1.1
    
    # Calculate below/above range percentages
    for name, df in [('clever', clever_df), ('billa', billa_df), 
                     ('ja', ja_df), ('other_brands', other_df)]:
        below = len(df[df[price_col] < range_vals[0]])
        above = len(df[df[price_col] > range_vals[1]])
        total = len(df)
        
        stats[period][name] = {
            'count': total,
            'below_pct': (below / total) * 100 if total > 0 else 0,
            'above_pct': (above / total) * 100 if total > 0 else 0
        }
    
    print(f"Number of Bins: {stats[period]['bins']}, Min Range: {stats[period]['range_min']:.2f}%, Max Range: {stats[period]['range_max']:.2f}%")
    
    # Generate plots
    x_ticks = np.arange(int(range_vals[0]/5)*5, int(range_vals[1]/5)*5 + 1, 5)
    
    # 1. Overlaid KDE plot
    plt.figure(figsize=(10, 6))
    sns.kdeplot(clever_df[price_col], color='skyblue', 
                label=f'Clever ({len(clever_df)} products)', 
                common_norm=False, linewidth=2.5)
    sns.kdeplot(billa_df[price_col], color='gold', 
                label=f'BILLA ({len(billa_df)} products)', 
                common_norm=False)
    sns.kdeplot(ja_df[price_col], color='lightgreen', 
                label=f'Ja! ({len(ja_df)} products)', 
                common_norm=False)
    sns.kdeplot(other_df[price_col], color='lightcoral', 
                label=f'Other Brands ({len(other_df)} products)', 
                common_norm=False)
    plt.title(f'Nahrungsmittel: {period_label} Price Changes (%) per Product Category')
    plt.xlabel(f'Price Change versus {period_label.split("-")[0]} ago (%)')
    plt.ylabel('Percentage of Products (%, smooth)')
    plt.xlim(range_vals[0], range_vals[1])
    plt.xticks(x_ticks)
    plt.legend()
    plt.axvline(0, color='black', linestyle='-', linewidth=1)
    plt.savefig(f'{OUTPUT_DIR}/{period}_overlay_kde.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 2-5. Individual histograms with KDE
    for name, df, color, label in [
        ('clever', clever_df, 'skyblue', 'Clever'),
        ('billa', billa_df, 'gold', 'BILLA'),
        ('ja', ja_df, 'lightgreen', 'Ja!'),
        ('other_brands', other_df, 'lightcoral', 'Other Brands')
    ]:
        plt.figure(figsize=(10, 6))
        sns.histplot(df[price_col], bins=bin_edges, kde=True, color=color, 
                    stat='percent', edgecolor=None, shrink=0.8, discrete=False)
        plt.title(f'Distribution of {period_label} Price Changes (%) for {label} Products (Relative Percentage)')
        plt.xlabel('Price Change (%)')
        plt.ylabel('Percentage of Products (%)')
        plt.xlim(range_vals[0], range_vals[1])
        plt.ylim(0, y_limit)
        plt.xticks(x_ticks)
        plt.axvline(0, color='black', linestyle='-', linewidth=1)
        plt.savefig(f'{OUTPUT_DIR}/{period}_{name}_histogram.png', dpi=150, bbox_inches='tight')
        plt.close()

# Process all three time periods
print("Generating plots for all time periods...")
process_time_period('1yr', '1-Year', merged_clever_1yr_df, merged_billa_1yr_df, 
                   merged_ja_1yr_df, merged_other_brands_1yr_df)
process_time_period('2yr', '2-Year', merged_clever_2yr_df, merged_billa_2yr_df, 
                   merged_ja_2yr_df, merged_other_brands_2yr_df)
process_time_period('3yr', '3-Year', merged_clever_3yr_df, merged_billa_3yr_df, 
                   merged_ja_3yr_df, merged_other_brands_3yr_df)

# Generate HTML report
print("Generating HTML report...")

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cheapflation Analysis - Nahrungsmittel Price Changes</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }}
        
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem 0;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 20px;
        }}
        
        h1 {{
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }}
        
        .subtitle {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}
        
        .update-time {{
            margin-top: 1rem;
            font-size: 0.9rem;
            opacity: 0.8;
        }}
        
        nav {{
            background: white;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        
        nav ul {{
            list-style: none;
            display: flex;
            justify-content: center;
            padding: 1rem 0;
        }}
        
        nav li {{
            margin: 0 1rem;
        }}
        
        nav a {{
            text-decoration: none;
            color: #667eea;
            font-weight: 600;
            padding: 0.5rem 1rem;
            border-radius: 5px;
            transition: all 0.3s ease;
        }}
        
        nav a:hover {{
            background: #667eea;
            color: white;
        }}
        
        .summary-box {{
            background: white;
            border-radius: 10px;
            padding: 1.5rem;
            margin: 2rem 0;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-top: 1rem;
        }}
        
        .summary-item {{
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}
        
        .summary-item h4 {{
            color: #667eea;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
        }}
        
        .summary-item .value {{
            font-size: 1.5rem;
            font-weight: bold;
            color: #333;
        }}
        
        section {{
            margin: 3rem 0;
        }}
        
        h2 {{
            color: #333;
            font-size: 2rem;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 3px solid #667eea;
        }}
        
        h3 {{
            color: #555;
            font-size: 1.3rem;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}
        
        .plot-container {{
            background: white;
            border-radius: 10px;
            padding: 1.5rem;
            margin: 1.5rem 0;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        
        .plot-container img {{
            width: 100%;
            height: auto;
            border-radius: 5px;
        }}
        
        .stats-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 1rem 0;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        
        .stats-table th {{
            background: #667eea;
            color: white;
            padding: 1rem;
            text-align: left;
            font-weight: 600;
        }}
        
        .stats-table td {{
            padding: 1rem;
            border-bottom: 1px solid #e9ecef;
        }}
        
        .stats-table tr:last-child td {{
            border-bottom: none;
        }}
        
        .stats-table tr:hover {{
            background-color: #f8f9fa;
        }}
        
        footer {{
            background: #333;
            color: white;
            text-align: center;
            padding: 2rem 0;
            margin-top: 4rem;
        }}
        
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 4px;
            font-size: 0.85rem;
            font-weight: 600;
        }}
        
        .badge-info {{
            background: #e3f2fd;
            color: #1976d2;
        }}
        
        @media (max-width: 768px) {{
            h1 {{
                font-size: 1.8rem;
            }}
            
            nav ul {{
                flex-direction: column;
                align-items: center;
            }}
            
            nav li {{
                margin: 0.5rem 0;
            }}
        }}
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>🛒 Cheapflation Analysis</h1>
            <p class="subtitle">Price Change Analysis for Nahrungsmittel Products at BILLA</p>
            <p class="update-time">Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </div>
    </header>
    
    <nav>
        <ul>
            <li><a href="#overview">Overview</a></li>
            <li><a href="#1yr">1-Year Analysis</a></li>
            <li><a href="#2yr">2-Year Analysis</a></li>
            <li><a href="#3yr">3-Year Analysis</a></li>
        </ul>
    </nav>
    
    <main class="container">
        <section id="overview">
            <div class="summary-box">
                <h2>Overview</h2>
                <p>This report analyzes price changes for food products (Nahrungsmittel) across different brand categories at BILLA over 1, 2, and 3-year periods.</p>
                
                <div class="summary-grid">
                    <div class="summary-item">
                        <h4>Total Products</h4>
                        <div class="value">{stats['total_products']}</div>
                    </div>
                    <div class="summary-item">
                        <h4>1-Year Data</h4>
                        <div class="value">{stats['1yr']['total']}</div>
                    </div>
                    <div class="summary-item">
                        <h4>2-Year Data</h4>
                        <div class="value">{stats['2yr']['total']}</div>
                    </div>
                    <div class="summary-item">
                        <h4>3-Year Data</h4>
                        <div class="value">{stats['3yr']['total']}</div>
                    </div>
                </div>
            </div>
        </section>
"""

# Add sections for each time period
for period, label in [('1yr', '1-Year'), ('2yr', '2-Year'), ('3yr', '3-Year')]:
    period_stats = stats[period]
    
    html_content += f"""
        <section id="{period}">
            <h2>{label} Price Change Analysis</h2>
            
            <div class="summary-box">
                <h3>Summary Statistics</h3>
                <p><span class="badge badge-info">Bins: {period_stats['bins']}</span> 
                   <span class="badge badge-info">Range: {period_stats['range_min']:.2f}% to {period_stats['range_max']:.2f}%</span></p>
                
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Brand Category</th>
                            <th>Product Count</th>
                            <th>Below Range</th>
                            <th>Above Range</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td><strong>Clever</strong></td>
                            <td>{period_stats['clever']['count']}</td>
                            <td>{period_stats['clever']['below_pct']:.2f}%</td>
                            <td>{period_stats['clever']['above_pct']:.2f}%</td>
                        </tr>
                        <tr>
                            <td><strong>BILLA</strong></td>
                            <td>{period_stats['billa']['count']}</td>
                            <td>{period_stats['billa']['below_pct']:.2f}%</td>
                            <td>{period_stats['billa']['above_pct']:.2f}%</td>
                        </tr>
                        <tr>
                            <td><strong>Ja!</strong></td>
                            <td>{period_stats['ja']['count']}</td>
                            <td>{period_stats['ja']['below_pct']:.2f}%</td>
                            <td>{period_stats['ja']['above_pct']:.2f}%</td>
                        </tr>
                        <tr>
                            <td><strong>Other Brands</strong></td>
                            <td>{period_stats['other_brands']['count']}</td>
                            <td>{period_stats['other_brands']['below_pct']:.2f}%</td>
                            <td>{period_stats['other_brands']['above_pct']:.2f}%</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            
            <h3>Comparison of All Brand Categories</h3>
            <div class="plot-container">
                <img src="{period}_overlay_kde.png" alt="{label} Price Changes - All Categories">
            </div>
            
            <h3>Individual Brand Category Distributions</h3>
            
            <div class="plot-container">
                <h4>Clever Products</h4>
                <img src="{period}_clever_histogram.png" alt="{label} Price Changes - Clever">
            </div>
            
            <div class="plot-container">
                <h4>BILLA Products</h4>
                <img src="{period}_billa_histogram.png" alt="{label} Price Changes - BILLA">
            </div>
            
            <div class="plot-container">
                <h4>Ja! Products</h4>
                <img src="{period}_ja_histogram.png" alt="{label} Price Changes - Ja!">
            </div>
            
            <div class="plot-container">
                <h4>Other Brands</h4>
                <img src="{period}_other_brands_histogram.png" alt="{label} Price Changes - Other Brands">
            </div>
        </section>
"""

html_content += """
    </main>
    
    <footer>
        <div class="container">
            <p>Cheapflation Analysis | Data source: <a href="https://github.com/ultralazr/billatracker" style="color: #667eea;">BILLA Tracker</a></p>
            <p style="margin-top: 0.5rem; font-size: 0.9rem; opacity: 0.8;">Generated automatically via GitHub Actions</p>
        </div>
    </footer>
</body>
</html>
"""

# Write HTML file
with open(f'{OUTPUT_DIR}/index.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"\n✅ Analysis complete! Generated files in '{OUTPUT_DIR}/' directory:")
print(f"   - index.html (main report)")
print(f"   - 15 plot images (5 per time period)")
print(f"\nAll files from previous run have been replaced.")
