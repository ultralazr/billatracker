# -*- coding: utf-8 -*-
"""
Billa Product Price Scraper
Refactored for GitHub Actions execution
"""

import requests
import pandas as pd
import json
import os
import logging
from datetime import datetime
import time
import pytz
from fake_useragent import UserAgent

# Configure logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_initial_page_data(start_url):
    """Fetch the first page to get total records count."""
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    
    try:
        response = requests.get(start_url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching initial data: {e}")
        return None


def replace_page_parameter(url, page_number):
    """Replace the 'page=' parameter in a URL with a given page number."""
    url_parts = url.split('&')
    new_url_parts = []
    for part in url_parts:
        if part.startswith('page='):
            new_url_parts.append('page=' + str(page_number))
        else:
            new_url_parts.append(part)
    return '&'.join(new_url_parts)


def scrape_all_pages(start_url, total_records, records_scraped, page_size=100, max_retries=5):
    """
    Scrapes all pages of the API based on total records and records already scraped.
    
    Args:
        start_url: The initial URL to start scraping from.
        total_records: The total number of records available in the API.
        records_scraped: The number of records already scraped.
        page_size: The number of records per page (default is 100).
        max_retries: Maximum number of retries for a failed page (default is 5).
    
    Returns:
        A list containing all the scraped product data.
    """
    product_list = []
    current_page = int(records_scraped / page_size)
    total_pages = (total_records + page_size - 1) // page_size

    while records_scraped < total_records:
        ua = UserAgent()
        headers = {'User-Agent': ua.random}
        current_url = replace_page_parameter(start_url, current_page)

        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(current_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                if 'results' in data:
                    for product_data in data['results']:
                        product_list.append(product_data)
                    records_in_response = len(data['results'])
                    records_scraped += records_in_response
                    records_to_go = total_records - records_scraped

                    logger.info(f"Completed: {records_scraped} / To go: {records_to_go}")
                    logger.info(f"Records in product_list: {len(product_list)}")

                current_page += 1
                time.sleep(2)
                break

            except requests.exceptions.RequestException as e:
                retries += 1
                logger.warning(f"Error fetching data from {current_url}: {e}. Retrying in 3 seconds... (Attempt {retries}/{max_retries})")
                time.sleep(3)

        if retries == max_retries:
            logger.error(f"Failed to scrape page {current_page} after {max_retries} retries. Stopping scraping.")
            break

    return product_list


def load_combined_dataframe(local_path='updated_combined_dataframe.csv'):
    """Load combined dataframe from local file or remote URL."""
    if os.path.exists(local_path):
        logger.info(f"Loading combined dataframe from local file: {local_path}")
        return pd.read_csv(local_path, low_memory=False)
    else:
        logger.info(f"Local file not found. Attempting to load from GitHub...")
        url = "https://raw.githubusercontent.com/ultralazr/billatracker/main/updated_combined_dataframe.csv"
        try:
            return pd.read_csv(url, low_memory=False)
        except Exception as e:
            logger.error(f"Error loading from remote URL: {e}")
            logger.info("Creating empty dataframe for first run...")
            return pd.DataFrame()


def extract_product_data(product_list):
    """Extract and flatten product data from scraped results."""
    flattened_product_df = pd.json_normalize(product_list, sep='_')
    
    columns_to_extract_map = {
        'sku': 'sku',
        'price_regular_value': 'price_regular_value',
        'bundleInfo': 'bundleInfo',
        'bundleSize': 'bundleSize',
        'inPromotion': 'inPromotion',
        'price_regular_promotionQuantity': 'price_regular_promotionQuantity',
        'price_regular_promotionText': 'price_regular_promotionText',
        'price_regular_promotionType': 'price_regular_promotionType',
        'price_crossed': 'price_crossed',
        'amount': 'amount',
        'volumeLabelShort': 'volumeLabelShort',
        'packageLabel': 'packageLabel',
        'conversionFactor': 'conversionFactor',
        'price_discountPercentage': 'price_discountPercentage',
        'price_regular_perStandardizedQuantity': 'price_regular_perStandardizedQuantity',
        'price_regular_tags': 'price_regular_tags',
        'price_regular_promotionValue': 'price_regular_promotionValue'
    }
    
    desired_column_order = ['sku', 'date'] + [col for col in columns_to_extract_map.values() if col != 'sku']
    new_price_history_entries = pd.DataFrame(columns=desired_column_order)
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for old_col, new_col in columns_to_extract_map.items():
        if old_col in flattened_product_df.columns:
            new_price_history_entries[new_col] = flattened_product_df[old_col]
        else:
            new_price_history_entries[new_col] = None
    
    new_price_history_entries['date'] = current_date
    
    # Format price columns
    price_columns_to_format = ['price_regular_value', 'price_crossed', 'price_regular_perStandardizedQuantity', 'price_regular_promotionValue']
    for col in price_columns_to_format:
        if col in new_price_history_entries.columns:
            new_price_history_entries[col] = pd.to_numeric(new_price_history_entries[col], errors='coerce')
            new_price_history_entries[col] = (new_price_history_entries[col] / 100).round(2)
    
    return new_price_history_entries


def find_differences(latest_historical_data, new_scraped_data):
    """Compare latest historical data with newly scraped data."""
    columns_to_compare = [col for col in new_scraped_data.columns if col not in ['sku', 'date']]
    differences = {}
    
    for index, row in latest_historical_data.iterrows():
        sku = row['sku']
        scraped_row = new_scraped_data[new_scraped_data['sku'] == sku]
        
        if scraped_row.empty:
            continue
        
        scraped_row = scraped_row.iloc[0]
        sku_differences = {}
        
        for col in columns_to_compare:
            hist_value = row.get(col)
            scrap_value = scraped_row.get(col)
            
            # Convert list-like objects to strings for comparison
            if isinstance(hist_value, list):
                hist_value_compare = str(hist_value)
            else:
                hist_value_compare = hist_value
            
            if isinstance(scrap_value, list):
                scrap_value_compare = str(scrap_value)
            else:
                scrap_value_compare = scrap_value
            
            # Treat None, empty strings, and NaN as equivalent
            if pd.isna(hist_value_compare) or hist_value_compare in ['', 'None']:
                hist_value_compare = None
            if pd.isna(scrap_value_compare) or scrap_value_compare in ['', 'None']:
                scrap_value_compare = None
            
            # Handle amount field with float comparison
            if col == 'amount':
                try:
                    hist_num = float(hist_value_compare) if hist_value_compare is not None else None
                    scrap_num = float(scrap_value_compare) if scrap_value_compare is not None else None
                    if hist_num == scrap_num:
                        continue
                except (ValueError, TypeError):
                    pass
            
            # Compare values
            if hist_value_compare != scrap_value_compare:
                sku_differences[col] = {
                    'historical': row[col],
                    'scraped': scraped_row[col]
                }
        
        if sku_differences:
            differences[sku] = sku_differences
    
    return differences


def update_log_file(log_filename, log_entry):
    """Load, update, and save the JSON log file."""
    if os.path.exists(log_filename):
        with open(log_filename, 'r') as f:
            log_data = json.load(f)
    else:
        log_data = []
    
    log_data.append(log_entry)
    
    with open(log_filename, 'w') as f:
        json.dump(log_data, f, indent=4)
    
    print(f"JSON log file '{log_filename}' updated successfully.")


def generate_html_report(log_filename, output_filename='scraping_summary.html'):
    """Generate HTML report from log file."""
    if not os.path.exists(log_filename):
        print(f"Log file '{log_filename}' not found.")
        return
    
    with open(log_filename, 'r') as f:
        log_data = json.load(f)
    
    # Sort by timestamp in descending order
    log_data.sort(key=lambda x: datetime.fromisoformat(x['timestamp']), reverse=True)
    
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Scraping Log</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .log-entry {{ border: 1px solid #ccc; padding: 15px; margin-bottom: 15px; background-color: white; border-radius: 5px; }}
        .log-entry h3 {{ margin-top: 0; color: #333; }}
        .differences {{ margin-top: 10px; padding: 10px; background-color: #f0f0f0; border-radius: 3px; }}
        .differences h4 {{ margin-top: 0; }}
    </style>
</head>
<body>
    <h1>Billa Scraper - Log Report</h1>
    {}
</body>
</html>
"""
    
    log_entries_html = []
    for entry in log_data:
        timestamp = entry.get('timestamp', 'N/A')
        total_records = entry.get('TotalRecords', 'N/A')
        records_scraped = entry.get('RecordsScraped', 'N/A')
        num_rows_combined = entry.get('Number of rows in combined_dataframe', 'N/A')
        num_rows_updated = entry.get('Number of rows in updated_combined_dataframe', 'N/A')
        differences_data = entry.get('Differences found', {})
        
        differences_html = ""
        if isinstance(differences_data, dict) and differences_data:
            differences_html += "<div class='differences'><h4>Differences Found:</h4>"
            for sku, diffs in differences_data.items():
                differences_html += f"<p><strong>SKU: {sku}</strong></p><ul>"
                for col, values in diffs.items():
                    differences_html += f"<li>{col}: Historical = {values.get('historical', 'N/A')}, Scraped = {values.get('scraped', 'N/A')}</li>"
                differences_html += "</ul>"
            differences_html += "</div>"
        elif isinstance(differences_data, str):
            differences_html += f"<div class='differences'><p>{differences_data}</p></div>"
        
        log_entry_html = f"""
    <div class="log-entry">
        <h3>Log Entry: {timestamp}</h3>
        <p><strong>Total Records:</strong> {total_records}</p>
        <p><strong>Records Scraped:</strong> {records_scraped}</p>
        <p><strong>Rows in Combined DataFrame:</strong> {num_rows_combined}</p>
        <p><strong>Rows in Updated Combined DataFrame:</strong> {num_rows_updated}</p>
        {differences_html}
    </div>
    """
        log_entries_html.append(log_entry_html)
    
    final_html_content = html_template.format("".join(log_entries_html))
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(final_html_content)
    
    print(f"HTML log file '{output_filename}' created successfully.")


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("Billa Price Scraper - Starting")
    logger.info("=" * 60)
    
    # Configuration
    START_URL = 'https://shop.billa.at/api/products?sortBy=relevance&page=0&pageSize=100'
    CSV_FILE = 'updated_combined_dataframe.csv'
    LOG_FILE = 'scraping_log.json'
    
    # Step 1: Get initial page data
    print("\n[1/7] Fetching initial page data...")
    initial_data = get_initial_page_data(START_URL)
    if not initial_data:
        print("Failed to fetch initial data. Exiting.")
        return
    
    total_records = initial_data.get('total', 0)
    print(f"Total Records Available: {total_records}")
    
    # Step 2: Scrape all pages
    print("\n[2/7] Scraping all pages...")
    product_list = scrape_all_pages(START_URL, total_records, 0)
    print(f"Total Products Scraped: {len(product_list)}")
    
    # Step 3: Extract and format product data
    print("\n[3/7] Extracting and formatting product data...")
    new_price_history_entries = extract_product_data(product_list)
    print(f"New Entries Created: {len(new_price_history_entries)}")
    print(new_price_history_entries.head())
    
    # Step 4: Load combined dataframe
    print("\n[4/7] Loading combined dataframe...")
    combined_dataframe = load_combined_dataframe(CSV_FILE)
    print(f"Existing Records: {len(combined_dataframe)}")
    
    # Step 5: Find differences
    print("\n[5/7] Comparing data and finding differences...")
    combined_dataframe['date'] = pd.to_datetime(combined_dataframe['date'], format='mixed')
    combined_dataframe_sorted = combined_dataframe.sort_values(by=['sku', 'date'])
    latest_historical_data = combined_dataframe_sorted.groupby('sku').last().reset_index()
    
    differences = find_differences(latest_historical_data, new_price_history_entries)
    
    if differences:
        print(f"Differences found: {len(differences)} SKUs with changes")
        for sku, diffs in differences.items():
            print(f"  SKU: {sku}")
            for col, values in diffs.items():
                print(f"    - {col}: {values['historical']} → {values['scraped']}")
    else:
        print("No differences found.")
    
    # Step 6: Update combined dataframe
    print("\n[6/7] Updating combined dataframe...")
    skus_with_differences = list(differences.keys())
    if skus_with_differences:
        records_to_append = new_price_history_entries[new_price_history_entries['sku'].isin(skus_with_differences)].copy()
        updated_combined_dataframe = pd.concat([combined_dataframe, records_to_append], ignore_index=True)
    else:
        updated_combined_dataframe = combined_dataframe
    
    updated_combined_dataframe.to_csv(CSV_FILE, index=False)
    print(f"Updated CSV saved: {CSV_FILE}")
    print(f"Before: {len(combined_dataframe)} rows")
    print(f"After: {len(updated_combined_dataframe)} rows")
    
    # Step 7: Create log entry and HTML report
    print("\n[7/7] Creating log entry and HTML report...")
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log_entry = {
        'timestamp': current_timestamp,
        'TotalRecords': int(total_records),
        'RecordsScraped': len(product_list),
        'Number of rows in combined_dataframe': len(combined_dataframe),
        'Number of rows in updated_combined_dataframe': len(updated_combined_dataframe),
        'Differences found': differences if differences else "No differences found between the latest historical data and the scraped data."
    }
    
    update_log_file(LOG_FILE, log_entry)
    generate_html_report(LOG_FILE)
    
    print("\n" + "=" * 60)
    print("Scraping Completed Successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
