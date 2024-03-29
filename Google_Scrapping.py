import re
import pandas as pd
import os
import urllib.parse
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Function to create Google Shopping link
def create_google_shopping_link(upc, title):
    base_url = "https://www.google.com/search?"
    query_parameters = {
        "q": f"{upc} {title}",
        "tbm": "shop"
    }
    encoded_query = urllib.parse.urlencode(query_parameters, quote_via=urllib.parse.quote_plus)
    return base_url + encoded_query

# Function to clean and convert price from string to float
def clean_and_convert_price(value):
    if pd.notna(value):
        if isinstance(value, str):
            cleaned_value = value.replace('$', '').replace(',', '').strip()
            try:
                return round(float(cleaned_value), 2)
            except ValueError:
                return None
        elif isinstance(value, float):
            return round(value, 2)
    return None


# Function to extract href attribute
def extract_href(url, master_db, forward_headers=True):
    # ScrapingBee API parameters
    api_key = '7WX8OW6NE4303BOGNO05RWHKP0COKSO0ZXZDF4Y0FKBZ5G7HDS5276Z1MCV9IU2EFRQC14OCU4AR7VI7'  # Replace with your ScrapingBee API key
    base_api_url = 'https://app.scrapingbee.com/api/v1/'

    # Headers required by ScrapingBee
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Spb-User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Spb-Referer": url,  # Example URL for referer
    }

    # Parameters for ScrapingBee API request
    params = {
        'api_key': api_key,
        'url': url,
        'forward_headers': 'true',  # Forward custom headers
        'render_js': 'true',  # If JavaScript rendering is required
        'custom_google': 'true'  # Custom parameter for Google Shopping
    }
    try:
        response = requests.get(base_api_url, params=params, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            element = soup.find('a', class_="DKkjqf")
            if element and element.has_attr('href'):
                href = element['href']
                # Extract product ID from href
                product_id = href.split('/shopping/product/')[1].split('?')[0]
                # Extract UPC from the original URL
                upc_match = re.search(r'q=(\d+)', url)
                upc = upc_match.group(1) if upc_match else "Unknown"

                # Check if ASIN is in Master_DB
                asin_match = re.search(r'dp/(\w+)', href)
                asin = asin_match.group(1) if asin_match else "Unknown"

                existing_row = master_db[(master_db['ASIN'] == asin) & (master_db['Extraction_Link'].notna())]

                if not existing_row.empty:
                    # ASIN is in Master_DB, proceed to scan every single row
                    for _, row in master_db.iterrows():
                        if row['ASIN'] == asin:
                            return row['Extraction_Link']

                    # ASIN is not in Master_DB, proceed with scraping
                    # Construct the final URL
                    final_url = f"https://www.google.com/shopping/product/{product_id}/offers?q={upc}&prds=cid:{product_id},cond:1"
                    return final_url
                else:
                    # ASIN is not in Master_DB, proceed with scraping
                    # Construct the final URL
                    final_url = f"https://www.google.com/shopping/product/{product_id}/offers?q={upc}&prds=cid:{product_id},cond:1"
                    return final_url
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

# Function to find the lowest price store
def find_lowest_price_store_with_scrapingbee(product_url):
    # Your ScrapingBee API key
    api_key = '7WX8OW6NE4303BOGNO05RWHKP0COKSO0ZXZDF4Y0FKBZ5G7HDS5276Z1MCV9IU2EFRQC14OCU4AR7VI7'
    # The URL to scrape, dynamically passed to the function
    url = product_url

    # Sending request to ScrapingBee API
    response = requests.get(
        'https://app.scrapingbee.com/api/v1/',
        params={
            'api_key': api_key,
            'url': url,
            'render_js': 'true',  # If you need to render JavaScript
            'custom_google': 'true'  # Custom parameter for Google Shopping
        }
    )

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        lowest_price = None
        lowest_price_store = None

        # List of additional stores to consider
        additional_stores = ['Sears - BHFO', 'Shop Premium Outlets', 'Walmart - BHFO, Inc.']

        # Iterate over each offer row
        offer_rows = soup.find_all('tr', class_='sh-osd__offer-row')

        for row in offer_rows:
            store_name_tag = row.find('a', class_='b5ycib shntl')
            quality_tag = row.find('span', class_='P6GC4b')
            price_tag = row.find('span', class_='g9WBQb fObmGc')
            shipping_info = row.find('td', class_='SH30Lb yGibJf')

            if store_name_tag and price_tag and shipping_info:
                store_name_raw = store_name_tag.get_text(strip=True)
                store_name = store_name_raw.split('Opens')[0].strip()
                price = float(price_tag.get_text(strip=True).replace('$', ''))

                # Exclude Amazon from the lowest price calculation
                if 'amazon' in store_name.lower():
                    continue

                # Extract shipping cost
                shipping_text = shipping_info.get_text(strip=True).lower()
                shipping_cost = calculate_shipping_cost(shipping_text)

                total_price = price + shipping_cost
                total_price = round(total_price, 2)


                if lowest_price is None or total_price < lowest_price:
                    lowest_price = total_price
                    lowest_price_store = store_name

        return lowest_price_store, lowest_price
    else:
        print(f"ScrapingBee request failed with status code {response.status_code}")
        return None, None

# Function to calculate shipping cost
def calculate_shipping_cost(shipping_text):
    if 'delivery by' in shipping_text:
        return 10.00
    elif 'free delivery' in shipping_text or 'free shipping' in shipping_text:
        return 0
    elif '$' in shipping_text:
        return float(shipping_text.split('$')[1].split()[0])
    else:
        return 0

# Function to check if the store is preferred
def is_preferred_store(quality_tag, store_name, additional_stores):
    is_top_quality = quality_tag and quality_tag.get_text(strip=True) == 'Top Quality Store'
    is_additional_store = any(store in store_name for store in additional_stores)
    return is_top_quality or is_additional_store

# Function to calculate Amazon list price
def calculate_amazon_list_price(row):
    if pd.isna(row['Price']) or row['Lowest FBM Seller'] == 'QualitySupplyCo (91% ANZYNJW9IIF9C)':
        return None

    buy_box_current = row['Buy Box: Current']
    new_current = row['New: Current']
    new_highest = row['New: Highest']
    # Convert 'Buy Box: Current' and 'New: Current' to float
    try:
        buy_box_current = float(buy_box_current) if pd.notna(buy_box_current) else None
    except ValueError:
        buy_box_current = None

    try:
        new_current = float(new_current) if pd.notna(new_current) else None
    except ValueError:
        new_current = None
    
    try:
        new_highest = float(new_highest) if pd.notna(new_highest) else None
    except ValueError:
        new_highest = None

    if buy_box_current is not None and buy_box_current > row['Min Price']:
        return buy_box_current - 1
    elif new_current is not None and new_current > row['Min Price']:
        return new_current - 1
    elif new_highest is not None:
        if row['Min Price'] > new_highest:
            return row['Min Price']
        elif row['Max Price'] < new_highest:
            return row['Max Price']
        else:
            return new_highest
    elif buy_box_current is None and new_current is None and new_highest is None:
        return row['Max Price']
    else:
        None



# Function to check if there is a match based on EAN code and extraction link
def check_match(row):
    ean_code = str(row['Product Codes: EAN']).lstrip('0').strip()
    extraction_link = str(row['Extraction_Link']).strip()
    return 'Y' if ean_code in extraction_link else 'N'

# Function to create an Amazon link
def create_amazon_link(asin):
    base_url = "https://www.amazon.com/dp/"
    return f"{base_url}{asin}?psc=1"

# Function to create a custom SKU
def create_custom_sku(upc):
    return "GB" + str(upc)

# Function to process a row with ScrapingBee
def process_row_with_scrapingbee(index, row, master_db, existing_output):

    # Check if ASIN already exists in master_db
    existing_row = master_db[master_db['ASIN'] == row['ASIN']]
    if not existing_row.empty and pd.notna(existing_row.iloc[0]['Extraction_Link']):
        # If ASIN exists and Extraction_Link is not NA, return the existing Extraction_Link
        existing_data = {
            'index': index,
            'Extraction_Link': existing_row.iloc[0]['Extraction_Link'],
            'Store': existing_row.iloc[0]['Store'],
            'Price': existing_row.iloc[0]['Price'],
            'ASIN': row['ASIN']
        }
        # Scrape store and price data for the existing ASIN
        google_link = create_google_shopping_link(row['Product Codes: UPC'], row['Title'])
        lowest_price_store, lowest_price = find_lowest_price_store_with_scrapingbee(existing_data['Extraction_Link'])
        existing_data['Store'] = lowest_price_store if lowest_price_store else "Not Found"
        existing_data['Price'] = lowest_price if lowest_price else pd.NA
        return existing_data
    else:
        # If ASIN does not exist or Extraction_Link is NA, fetch the Extraction_Link using ScrapingBee
        google_link = create_google_shopping_link(row['Product Codes: UPC'], row['Title'])
        extracted_href = extract_href(google_link, master_db)
        if extracted_href:
            lowest_price_store, lowest_price = find_lowest_price_store_with_scrapingbee(extracted_href)
            return {
                'index': index,
                'Extraction_Link': extracted_href,
                'Store': lowest_price_store if lowest_price_store else "Not Found",
                'Price': lowest_price if lowest_price else pd.NA,
                'ASIN': row['ASIN']
            }
        else:
            return {
                'index': index,
                'Extraction_Link': "Not Found",
                'Store': "Not Found",
                'Price': pd.NA,
                'ASIN': row['ASIN']
            }


# Function to update the Master_DB file with new data
def update_master_db(master_db, new_data):
    # Filter new data rows with Extraction_Link present and not equal to "Not Found"
    new_data_with_links = new_data[(new_data['Extraction_Link'].notna()) & (new_data['Extraction_Link'] != "Not Found")]

    # Extract unique ASINs from the master_db
    existing_asins = set(master_db['ASIN'])

    # List to store new data rows
    new_rows = []

    # Add new data to the list if the ASIN is not already present
    for index, row in new_data_with_links.iterrows():
        if row['ASIN'] not in existing_asins:
            new_rows.append(row)

    # Concatenate existing Master_DB with new rows
    if new_rows:
        master_db = pd.concat([master_db, pd.DataFrame(new_rows)], ignore_index=True)

    # Drop duplicate rows based on ASIN
    master_db.drop_duplicates(subset='ASIN', keep='first', inplace=True)

    # Add 'Blacklisted' column with initial value 'N'
    master_db['Blacklisted'] = 'N'

    # Save the updated Master_DB to a file
    master_db.to_csv(master_db_path, index=False)





# Main function
if __name__ == "__main__":
    start_time = time.time()

    # Load the existing Master_DB file
    master_db_path = 'Master_DB.csv'
    if os.path.exists(master_db_path):
        master_db = pd.read_csv(master_db_path)
    else:
        master_db = pd.DataFrame(columns=['ASIN', 'Extraction_Link', 'Blacklisted'])

    min_roi = 1.7
    max_roi = 1.89
    rows_to_run = 4000
    # Read the input CSV file
    file_path = 'Keepa_Combined_Export.csv'
    spreadsheet = pd.read_csv(file_path)
    # Dropping all 'Unnamed' columns from the DataFrame
    spreadsheet = spreadsheet.loc[:, ~spreadsheet.columns.str.contains('^Unnamed')]

    # Clean and convert 'New: Current' and 'Buy Box: Current' prices
    spreadsheet['New: Current'] = spreadsheet['New: Current'].apply(clean_and_convert_price)
    spreadsheet['Buy Box: Current'] = spreadsheet['Buy Box: Current'].apply(clean_and_convert_price)

    # Adds a custom SKU which is GB+SKU so we can later identify based on GB when listed
    spreadsheet['Custom_SKU'] = spreadsheet['Product Codes: UPC'].apply(create_custom_sku)

    # Generate Google Shopping and Amazon links for each row
    spreadsheet['Google_Search_Link'] = spreadsheet.apply(
        lambda row: create_google_shopping_link(row['Product Codes: UPC'], row['Title']), axis=1)

    # Creates the Amazon link for comparison
    spreadsheet['Amazon_link'] = spreadsheet['ASIN'].apply(create_amazon_link)

    # Check for an existing output file
    output_csv_path = 'North_Face_1_All_PRODUCTS.csv'
    if os.path.exists(output_csv_path):
        # Load the existing output file
        existing_output = pd.read_csv(output_csv_path)
    else:
        # If there's no existing output file, initialize an empty DataFrame
        existing_output = pd.DataFrame(columns=['Extraction_Link'])

    # Process all rows from the beginning
    rows_to_process = spreadsheet.head(rows_to_run)

    with ThreadPoolExecutor(max_workers=30) as executor:
        # Use ThreadPoolExecutor to process rows concurrently
        future_to_index = {executor.submit(process_row_with_scrapingbee, index, row, master_db, existing_output): index for index, row in rows_to_process.iterrows()}

        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                result = future.result()
                # Update the DataFrame with the result
                if result:
                    spreadsheet.at[index, 'Extraction_Link'] = result['Extraction_Link']
                    spreadsheet.at[index, 'Store'] = result['Store']
                    spreadsheet.at[index, 'Price'] = result['Price']

                    # Check if ASIN is not Blacklisted and not already in Master_DB
                    asin = result.get('ASIN')
                    if asin and asin not in master_db['ASIN'].values:
                        master_db = master_db.append({'ASIN': asin, 'Extraction_Link': result['Extraction_Link'], 'Blacklisted': 'N'}, ignore_index=True)

                    # Update existing_output DataFrame with the most recent links
                    if result['Extraction_Link'] not in existing_output['Extraction_Link'].values:
                        existing_output = existing_output.append({'Extraction_Link': result['Extraction_Link']}, ignore_index=True)
            except Exception as exc:
                print(f'Row {index} generated an exception: {exc}')

    # Perform the remaining calculations and updates to the DataFrame
    spreadsheet['Min Price'] = spreadsheet['Price'].apply(
        lambda x: round(x * min_roi, 2) if pd.notna(x) else None)
    spreadsheet['Max Price'] = spreadsheet['Price'].apply(
        lambda x: round(x * max_roi, 2) if pd.notna(x) else None)
    spreadsheet['Amazon_List_price'] = spreadsheet.apply(calculate_amazon_list_price, axis=1)
    spreadsheet['Can_List'] = spreadsheet['Amazon_List_price'].apply(lambda x: 'Y' if pd.notna(x) else 'N')
    spreadsheet.insert(18, 'Match', spreadsheet.apply(check_match, axis=1))

    # Save the updated DataFrame to a new CSV file
    output_csv_path = 'North_Face_1_All_PRODUCTS.csv'
    spreadsheet.to_csv(output_csv_path, index=False)

    # Update the Master_DB file with new data
    update_master_db(master_db, spreadsheet)

    # Filter rows with an extraction link and can_list column as 'Y'
    filtered_output = spreadsheet[(spreadsheet['Extraction_Link'].notna()) & (spreadsheet['Can_List'] == 'Y')]

    # Filter out duplicate ASINs
    filtered_output = filtered_output.drop_duplicates(subset='ASIN', keep='first')

    # Define the path for the filtered output CSV file
    filtered_output_csv_path = 'North_Face_1_All_PRODUCTS_can_list.csv'


    # Check if the filtered output CSV file already exists
    if os.path.exists(filtered_output_csv_path):
        # Load the existing file
        existing_filtered_output = pd.read_csv(filtered_output_csv_path)
        # Concatenate the existing and new data, drop duplicates, and update the file
        filtered_output = pd.concat([existing_filtered_output, filtered_output]).drop_duplicates(subset='ASIN', keep='first')

    # Save the filtered output to the same CSV file
    filtered_output.to_csv(filtered_output_csv_path, index=False)

    # Display success message
    print(f"Filtered data updated in '{filtered_output_csv_path}' successfully.")


    # Print the total time taken
    end_time = time.time()
    print(f"Total time taken: {end_time - start_time} seconds")

