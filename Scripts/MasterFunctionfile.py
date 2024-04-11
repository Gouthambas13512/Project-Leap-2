import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random 
import csv
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import MasterFunctionfile as mf
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse
import urllib
from ftplib import FTP
from ftplib import FTP_TLS



def find_lowest_price_store_with_scrapfly(product_url):
    api_key = 'scp-test-6fc24c20fe1f4ba0a171e7355e9ab34f'  # Replace with your actual Scrapfly API key
    additional_stores = ['Sears - BHFO', 'Shop Premium Outlets', 'Walmart - BHFO, Inc.','APerfectDealer', 'Van Dyke and Bacon', 'TC Running Co','eBay',"Macy's",'Kenco Outfitters','Grivet Outdoors','TravelCountry.com','EMS','Famous Brands','Walmart - BuyBox Club','Baseball Savings.com','Sears - Ricci Berri', 'Slam Jam', 'mjfootwear.com', 'Sports Basement', 'ModeSens','Runnerinn.com','ShoeVillage.com','Running Zone','The Heel Shoe Fitters','Nikys Sports',"Beck's Shoes",'Next Step Athletics', "Brown's Shoe Fit Co. Dubuque", 'Super Shoes', 'JosephBeauty', 'Pants Store', 'Fingerhut', "Brown's Shoe Fit Co. Longview", 'Deporvillage.net' ]
    Black_List_Store = ["Chiappetta Shoes", "Sole Desire", "Shoe Station", "Bloomingdale's", "Lucky Shoes", "Glik's", "RushOrderTees", "Shoe Carnival", "FrontRunners LA", "Roderer Shoe Center", "Rogans Shoes", "Goodmiles Running Company", "Gazelle Sports", "Confluence Running", "Holabird Sports", "ssense.com", "Lyst", "Fleet Feet"]

    scrapfly = ScrapflyClient(key=api_key)
    
    error_counter = 0
    
    try:
        result: ScrapeApiResponse = scrapfly.scrape(ScrapeConfig(
            tags=["player", "project:default"],
            country="us",
            asp=True,
            render_js=True,
            rendering_wait=3000,
            url=product_url
        ))
        
        if result.status_code == 200:
            soup = BeautifulSoup(result.content, 'html.parser')
            lowest_price = None
            lowest_price_store = None

            offer_rows = soup.find_all('tr', class_='sh-osd__offer-row')

            for row in offer_rows:
                try:
                    store_name_tag = row.find('a', class_='b5ycib shntl')
                    quality_tag = row.find('span', class_='P6GC4b')
                    price_tag = row.find('span', class_='g9WBQb fObmGc')
                    shipping_info = row.find('td', class_='SH30Lb yGibJf')

                    if store_name_tag and price_tag and shipping_info:
                        store_name_raw = store_name_tag.get_text(strip=True)
                        store_name = store_name_raw.split('Opens')[0].strip()
                        
                        # Check if the store is blacklisted
                        if any(bad_word in store_name for bad_word in Black_List_Store):
                            continue  # Skip this store and move to the next one
                        
                        price_str = price_tag.get_text(strip=True).replace('$', '').replace(',', '').replace('\xa0€', '').strip()
                        price = float(price_str)

                        if 'amazon' in store_name.lower():
                            continue

                        shipping_text = shipping_info.get_text(strip=True).lower()
                        shipping_cost = calculate_shipping_cost(shipping_text)

                        total_price = price + shipping_cost
                        total_price = round(total_price, 2)

                        if is_preferred_store(quality_tag, store_name, additional_stores):
                            if lowest_price is None or total_price < lowest_price:
                                lowest_price = total_price
                                lowest_price_store = store_name
                except Exception as inner_exc:
                    error_counter += 1
                    print(f"Error processing row: {inner_exc}")
                    continue

            return lowest_price_store, lowest_price
        else:
            error_counter += 1
            print(f"Scrapfly request failed with status code {result.status_code}")
    except Exception as e:
        error_counter += 1
        print(f"An error occurred: {e}")

    return None, None
            

def find_lowest_price_store_with_scrapingbee(product_url):
    api_key = '7WX8OW6NE4303BOGNO05RWHKP0COKSO0ZXZDF4Y0FKBZ5G7HDS5276Z1MCV9IU2EFRQC14OCU4AR7VI7'  # Use your actual API key
    additional_stores = ['Sears - BHFO', 'Shop Premium Outlets', 'Walmart - BHFO, Inc.','APerfectDealer', 'Van Dyke and Bacon', 'TC Running Co','eBay',"Macy's",'Kenco Outfitters','Grivet Outdoors','TravelCountry.com','EMS','Famous Brands','Walmart - BuyBox Club','Baseball Savings.com','Sears - Ricci Berri', 'Slam Jam', 'mjfootwear.com', 'Sports Basement', 'ModeSens','Runnerinn.com','ShoeVillage.com','Running Zone','The Heel Shoe Fitters','Nikys Sports',"Beck's Shoes",'Next Step Athletics', "Brown's Shoe Fit Co. Dubuque", 'Super Shoes', 'JosephBeauty', 'Pants Store', 'Fingerhut', "Brown's Shoe Fit Co. Longview", 'Deporvillage.net' ]
    
    # Initialize variables to keep track of the lowest price and its store
    lowest_price = None
    lowest_price_store = None
    # New variables to track any store found, regardless of being a "good" store
    any_store_found = None
    any_store_price = None
    error_counter = 0

    try:
        response = requests.get(
            'https://app.scrapingbee.com/api/v1/',
            params={
                'api_key': api_key,
                'url': product_url,
                'render_js': 'true',
                'custom_google': 'true',
                'wait_for': 'a[class*=title]'
            }
        )

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check for "Product not found" title
            if soup.title and 'Product not found' in soup.title.text:
                return "OOS", None

            offer_rows = soup.find_all('tr', class_='sh-osd__offer-row')
            
            for row in offer_rows:
                try:
                    store_name_tag = row.find('a', class_='b5ycib shntl')
                    quality_tag = row.find('span', class_='P6GC4b')
                    price_tag = row.find('span', class_='g9WBQb fObmGc')
                    shipping_info = row.find('td', class_='SH30Lb yGibJf')

                    if store_name_tag and price_tag and shipping_info:
                        store_name_raw = store_name_tag.get_text(strip=True)
                        store_name = store_name_raw.split('Opens')[0].strip()
                        price_str = price_tag.get_text(strip=True).replace('$', '').replace(',', '').strip()
                        price = float(price_str)

                        if 'amazon' in store_name.lower():
                            continue

                        shipping_text = shipping_info.get_text(strip=True).lower()
                        shipping_cost = calculate_shipping_cost(shipping_text)

                        total_price = price + shipping_cost
                        total_price = round(total_price, 2)

                        # Track any store found
                        if any_store_found is None or total_price < any_store_price:
                            any_store_found = store_name
                            any_store_price = total_price

                        if is_preferred_store(quality_tag, store_name, additional_stores):
                            if lowest_price is None or total_price < lowest_price:
                                lowest_price = total_price
                                lowest_price_store = store_name
                except Exception as inner_exc:
                    error_counter += 1
                    continue

            # Check if a "good" store was not found but any store was found
            if lowest_price_store is None and any_store_found:
                lowest_price_store = any_store_found
                lowest_price = None  # Set price to None as per requirement

            return lowest_price_store, lowest_price
        else:
            print(f"ScrapingBee request failed with status code {response.status_code}")
            return None, None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None


def is_preferred_store(quality_tag, store_name, additional_stores):
    is_top_quality = quality_tag and quality_tag.get_text(strip=True) == 'Top Quality Store'
    is_additional_store = any(store in store_name for store in additional_stores)
    return is_top_quality or is_additional_store

def calculate_shipping_cost(shipping_text):
    if 'Spend' in shipping_text:
        return 10.00
    elif 'Free delivery' in shipping_text or 'Free shipping' in shipping_text:
        return 0
    elif '$' in shipping_text:
        cost = float(shipping_text.split('$')[1].split()[0])
        if cost > 20:
            return 10.00
        else:
            return cost
    else:
        return 0
    
def calculate_amazon_list_price(row):
    if pd.isna(row['Price']):
        return None
    #might need to change so that it 
    if row['Lowest FBM Seller'] == 'ANZYNJW9IIF9C':
        return row['New: Current']

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
    if new_current is not None and new_current > row['Min Price']:
        return new_current - 1
    if new_highest is not None:
        if row['Min Price'] > new_highest:
            return None
        elif row['Max Price'] < new_highest:
            return row['Max Price']
        else:
            return new_highest
    if buy_box_current is None and new_current is None and new_highest is None:
        return row['Max Price']
    else:
        None

def Update_Can_List(row):
    import pandas as pd  # Ensure pandas is imported

    # If 'Min Price' is NaN, return "N"
    if pd.isna(row['Min Price']):
        return "N"

    # Attempt to convert 'Buy Box: Current' and 'New: Current' to float, handling exceptions
    try:
        buy_box_current = float(row['Buy Box: Current']) if pd.notna(row['Buy Box: Current']) else None
    except ValueError:
        buy_box_current = None

    try:
        new_current = float(row['New: Current']) if pd.notna(row['New: Current']) else None
    except ValueError:
        new_current = None

    # Determine if 'Min Price' is greater than either 'Buy Box: Current' or 'New: Current'
    if buy_box_current is not None or new_current is not None:
        if (buy_box_current is not None and row['Min Price'] > buy_box_current) or \
           (new_current is not None and row['Min Price'] > new_current):
            return "N"
    return "Y"

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


def update_master_v(keepa_file, master_file):
    # Step 1: Load Keepa_Update Data
    keepa_data = {}
    with open(keepa_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            asin = row['ASIN']
            # Store and clean Buy Box: Current, New: Current values for each ASIN, and store Lowest FBM Seller as is
            keepa_data[asin] = {
                'Buy Box: Current': clean_and_convert_price(row['Buy Box: Current']),
                'New: Current': clean_and_convert_price(row['New: Current']),
                'New: Highest': clean_and_convert_price(row['New: Highest']),
                'Lowest FBM Seller': row.get('Lowest FBM Seller')  # No cleaning needed
            }
    
    # Step 2: Read MasterV and Prepare Updated Data
    updated_rows = []
    with open(master_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames
        for row in reader:
            asin = row['ASIN']
            if asin in keepa_data:
                # Update values from Keepa_Update after cleaning and converting
                row['Buy Box: Current'] = keepa_data[asin]['Buy Box: Current']
                row['New: Current'] = keepa_data[asin]['New: Current']
                row['New: Highest'] = keepa_data[asin]['New: Highest']
                # Directly update Lowest FBM Seller
                row['Lowest FBM Seller'] = keepa_data[asin]['Lowest FBM Seller']
            updated_rows.append(row)
    
    # Step 3: Write Updates Back to MasterV
    with open(master_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)
    print("Done")


import os
import pandas as pd

def merge_csv(input_file):
    # Specify the output file name
    output_file = "KeepaExports/Keepa_Combined_Export.csv"
    
    # Check if the output file exists
    if os.path.exists(output_file):
        # Load existing data from output file
        existing_data = pd.read_csv(output_file)
    else:
        # Create an empty DataFrame if the output file doesn't exist
        existing_data = pd.DataFrame()

    # Read data from the input file
    input_data = pd.read_csv(input_file)

    # If 'ASIN' column exists in the existing data, remove duplicates based on 'ASIN'
    if 'ASIN' in existing_data.columns:
        input_data = input_data[~input_data['ASIN'].isin(existing_data['ASIN'])]

    # Read master database
    master_db_file = "DataBaseFiles/Master_DB.csv"
    if os.path.exists(master_db_file):
        master_db = pd.read_csv(master_db_file)
    else:
        master_db = pd.DataFrame()

    # Filter input data for ASINs not present in master database
    new_asins = input_data[~input_data['ASIN'].isin(master_db['ASIN'])]

    # Append new data to existing data
    merged_data = pd.concat([existing_data, new_asins], ignore_index=True)

    # Write merged data to the output file
    merged_data.to_csv(output_file, index=False)

    # Output only the new ASINs which are not duplicates
    print("New ASINs added to output file:")
    print(new_asins['ASIN'].tolist())

    print(f"Data from {input_file} merged successfully.")


def Update_DB_After_ManualV(input_file, master_db_file, master_v_file, Black_Listed_Y):
    # Load master database
    master_db = pd.read_csv(master_db_file)

    # Load or initialize MasterV file
    if os.path.exists(master_v_file):
        master_v = pd.read_csv(master_v_file)
    else:
        master_v = pd.DataFrame(columns=master_db.columns)
        master_v['Curr_match'] = 0

    # Initialize or load Black_Listed_Y file
    if os.path.exists(Black_Listed_Y):
        black_listed_y = pd.read_csv(Black_Listed_Y)
    else:
        black_listed_y = pd.DataFrame(columns=master_db.columns)

    # Load input file
    input_df = pd.read_csv(input_file)

    # Initialize counters for items added to master_v_file and black_listed_y file
    items_added_to_master_v = 0
    items_added_to_black_listed_y = 0

    # Process each ASIN from the input file
    for asin in input_df['ASIN']:
        # Find if the ASIN exists in master_db and its Blacklisted status
        asin_row = master_db[master_db['ASIN'] == asin]

        # If ASIN is found in master_db
        if not asin_row.empty:
            # Check if any of the entries for this ASIN in the 'Blacklisted' column contain 'Y' or 'V'
            blacklisted_entries = asin_row['Blacklisted'].str.upper().str.strip()
            if 'Y' in blacklisted_entries.values:
                # If ASIN is blacklisted with 'Y' and not already in Black_Listed_Y
                if asin not in black_listed_y['ASIN'].values:
                    new_row = asin_row.copy()
                    black_listed_y = pd.concat([black_listed_y, new_row], ignore_index=True)
                    items_added_to_black_listed_y += 1
            elif 'V' in blacklisted_entries.values:
                # If ASIN is blacklisted with 'V' and not already in master_v_file
                if asin not in master_v['ASIN'].values:
                    new_row = asin_row.copy()
                    master_v = pd.concat([master_v, new_row], ignore_index=True)
                    items_added_to_master_v += 1

    # If ASIN is not found or not blacklisted with 'V' or 'Y', add to manual_check
    for asin in input_df['ASIN']:
        if asin not in black_listed_y['ASIN'].values and asin not in master_v['ASIN'].values:
            input_row = input_df[input_df['ASIN'] == asin].copy()
            black_listed_y = pd.concat([black_listed_y, input_row], ignore_index=True)
            items_added_to_black_listed_y += 1

    # Remove duplicates based on 'ASIN' in black_listed_y
    black_listed_y.drop_duplicates(subset=['ASIN'], keep='last', inplace=True)

    # Save the updated files
    black_listed_y.to_csv(Black_Listed_Y, index=False)
    master_v.to_csv(master_v_file, index=False)

    # Print the number of items added to master_v_file and black_listed_y file
    print(f"{items_added_to_master_v} items were added to master_v_file.")
    print(f"{items_added_to_black_listed_y} items were added to black_listed_y file.")

    print("Files have been updated.")

def create_manualcheck_file(input_file):
    # Load MasterV.csv
    master_df = pd.read_csv("DataBaseFiles\MasterV.csv")
    
    # Load input file
    input_df = pd.read_csv(input_file)
    
    # Initialize a list to store rows that need manual check
    manual_check_rows = []
    
    # Iterate through ASINs in the input file
    for index, row in input_df.iterrows():
        asin = row['ASIN']
        # Check if ASIN exists in MasterV.csv
        if asin not in master_df['ASIN'].values:
            manual_check_rows.append(row)
    
    # If there are rows needing manual check, create manualcheck.csv
    if manual_check_rows:
        manual_check_df = pd.DataFrame(manual_check_rows)
        manual_check_df.to_csv("ManualCheck.csv", index=False)
        print("Manual check file created: manualcheck.csv")
    else:
        print("All ASINs found in MasterV.csv. No manual check needed.")
# Example usage:
    #create_manualcheck_file("")
        
import pandas as pd

def update_master_db_w_manualcheck(master_db_path, update_csv_path, manual_Update_Ouput):
    # Check if Master_DB.csv exists
    if os.path.exists(master_db_path):
        # Read Master_DB.csv
        master_db = pd.read_csv(master_db_path)
    else:
        # Create an empty DataFrame if Master_DB.csv doesn't exist
        master_db = pd.DataFrame(columns=['ASIN', 'Blacklisted', 'Extraction_Link'])

    # Read Update_CSV.csv
    update_csv = pd.read_csv(update_csv_path)

    # Iterate through rows in Update_CSV and update Master_DB accordingly
    for index, row in update_csv.iterrows():
        asin = row['ASIN']
        black_list = row.get('Black_list')
        product_code = row.get('PID')
        upc = row["Product Codes: UPC"]

        # Find the row in Master_DB that matches the ASIN
        master_row_index = master_db.index[master_db['ASIN'] == asin]

        if not master_row_index.empty:
            # ASIN found in Master_DB, update Blacklist if provided
            if black_list is not None:
                master_db.loc[master_row_index, 'Blacklisted'] = black_list
        else:
            # ASIN not found in Master_DB, add a new row
            new_row = pd.DataFrame({'ASIN': [asin], 'Blacklisted': [black_list], 'Extraction_Link': [None]})
            master_db = pd.concat([master_db, new_row], ignore_index=True)

        # Update Link if Product Code is provided and not 'None'
        if product_code is not None and pd.notna(product_code) and str(product_code).lower() != "none":
            new_link = f'https://www.google.com/shopping/product/{product_code}/offers?q={upc}&prds=cid:{product_code},cond:1'
            master_db.loc[master_row_index, 'Extraction_Link'] = new_link
        else:
            # If PID is None or 'None', set Extraction_Link to None
            master_db.loc[master_row_index, 'Extraction_Link'] = "None"

    # Save the updated Master_DB
    master_db.to_csv(manual_Update_Ouput, index=False)

    print(f"{manual_Update_Ouput} updated successfully.")



def process_row_with_scrapingbee(row, index):
    MIN_ROI = 1.7
    MAX_ROI = 1.87
    
    if pd.isna(row['Extraction_Link']) or row['Extraction_Link'] == "None":
        print(f"Skipping row {index} as Extraction_Link is None")
        return None

    print(f"Working on row {index}")
    # Initialize default values for the updates
    update_info = {
        'Store': None,
        'Price': None,
        'Min Price': None,
        'Max Price': None,
        'Amazon_List_price': None,
        'Can_List': "N"  # Default to "N", will update if conditions are met
    }

    product_url = row['Extraction_Link']
    # Attempt to find the lowest price store using the given product URL
    try:
        lowest_price_store, price = mf.find_lowest_price_store_with_scrapfly(product_url)

        if price is not None:
            # Calculate the Min Price, Max Price, and Amazon List Price based on the retrieved price
            min_price = price * MIN_ROI
            max_price = price * MAX_ROI
            amazon_list_price = mf.calculate_amazon_list_price({**row.to_dict(), 'Price': price, 'Min Price': min_price, 'Max Price': max_price})

            # Update the row information with the calculated values
            update_info.update({
                'Store': lowest_price_store,
                'Price': price,
                'Min Price': min_price,
                'Max Price': max_price,
                'Amazon_List_price': amazon_list_price,
                'Can_List': "Y"  # Update to "Y" since we have valid pricing information
            })
    except Exception as e:
        print(f"Error processing row {index}: {e}")
        # If an error occurs, retain default update_info which indicates failure

    # Return a tuple containing the index and the update information
    # This allows the calling function to know which row this information pertains to
    return (index, update_info)


def update_pricing_concurrently(Curr_Listed_path, master_db_path, max_workers):
    Curr_Listed = pd.read_csv(Curr_Listed_path)
    master_db = pd.read_csv(master_db_path)

    with ThreadPoolExecutor(20) as executor:
        # Submit tasks for each row
        futures = [executor.submit(process_row_with_scrapingbee, row, index) for index, row in Curr_Listed.iterrows()]

        # Process results as they complete
        for future in as_completed(futures):
            index, update_info = future.result()
            # Update the Curr_Listed DataFrame with the results
            for key, value in update_info.items():
                Curr_Listed.at[index, key] = value

    # After processing, save the updated DataFrames
    Curr_Listed.to_csv(Curr_Listed_path, index=False)
    print("Done")
    # Handle master_db updates outside of the concurrent processing block to ensure thread safety


def count_rows(csv_file):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)
        
        # Get the number of rows
        row_count = df.shape[0]
        
        print(f"Total rows in '{csv_file}': {row_count}")
    except FileNotFoundError:
        print(f"File '{csv_file}' not found.")



import pandas as pd
import os

def filter_and_export(csv_file, brand_names, output_filename):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)
        
        # Initialize an empty DataFrame to store filtered results
        filtered_df = pd.DataFrame()
        
        # Iterate over each brand name in the list
        for brand in brand_names:
            # Filter rows where Brand column matches the current brand name
            brand_df = df[df['Brand'].str.contains(brand)]
            # Append the filtered results to the overall filtered DataFrame
            filtered_df = pd.concat([filtered_df, brand_df])
        
        # Create the directory if it doesn't exist
        output_folder = "Filtered_outputs"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        # Construct the full output file path
        output_file_path = os.path.join(output_folder, output_filename)
        
        # Export the filtered DataFrame to a new CSV file
        filtered_df.to_csv(output_file_path, index=False)
        
        print(f"Filtered data exported to '{output_file_path}' successfully.")
    except FileNotFoundError:
        print(f"File '{csv_file}' not found.")

def create_google_shopping_link(upc, title):
    base_url = "https://www.google.com/search?"
    query_parameters = {
        "q": f"{upc} {title}",
        "tbm": "shop"
    }
    encoded_query = urllib.parse.urlencode(query_parameters, quote_via=urllib.parse.quote_plus)
    return base_url + encoded_query

def create_amazon_link(asin):
    base_url = "https://www.amazon.com/dp/"
    return f"{base_url}{asin}?psc=1"

import pandas as pd

def prepare_manual_only_import(input_file, output_file):
    # Read the input CSV file
    spreadsheet = pd.read_csv(input_file)

    # Save temp data
    temp_blacklisted = spreadsheet['Black_list'].copy()
    temp_pid = spreadsheet['PID'].copy()

    # Delete the columns
    del spreadsheet['Black_list']
    del spreadsheet['PID']

    # Define a function to process each row
    def process_row(row, blacklisted, pid):
        upc = row['Product Codes: UPC']
        
        row['Prime Eligible (Amazon offer)'] = None
        # Custom_SKU
        row['Custom_SKU'] = "GB" + str(upc)

        # Google_Search_Link
        row['Google_Search_Link'] = create_google_shopping_link(upc, row['Title'])

        # Amazon_link
        row['Amazon_link'] = create_amazon_link(row['ASIN'])

        # Match
        row['Match'] = "Y"

        # Use temp PID data for Extraction_Link
        product_code = pid[row.name]  # Using row index to match
        if product_code == "none":
            row['Extraction_Link'] = None
        else:
            row['Extraction_Link'] = f'https://www.google.com/shopping/product/{product_code}/offers?q={upc}&prds=cid:{product_code},cond:1'
        
        # Set other fields to None or initial values
        row['Store'] = None
        row['Price'] = None
        row['Min Price'] = None
        row['Max Price'] = None
        row['Amazon_List_price'] = None
        row['Can_List'] = "N"
        row['Curr_Listed?'] = 0

        # Use temp Blacklisted data
        row['Blacklisted'] = blacklisted[row.name]  # Using row index to match

        return row

    # Apply the function to each row, passing the temp data as additional arguments
    spreadsheet = spreadsheet.apply(lambda row: process_row(row, temp_blacklisted, temp_pid), axis=1)

    # Define the columns order and rearrange the DataFrame
    columns_order = [
        'Title', 'Sales Rank: 30 days avg.', 'Sales Rank: 90 days avg.', 'Buy Box: Current',
        'Buy Box: Stock', 'New: Current', 'New: Highest', 'Referral Fee %',
        'New, 3rd Party FBM: 30 days avg.', 'Lowest FBM Seller', 
        'Count of retrieved live offers: New, FBM', 'ASIN', 'Product Codes: EAN', 
        'Product Codes: UPC', 'Parent ASIN', 'Brand', 'Prime Eligible (Amazon offer)',
        'Custom_SKU', 'Google_Search_Link', 'Match', 'Amazon_link', 'Extraction_Link', 
        'Store', 'Price', 'Min Price', 'Max Price', 'Amazon_List_price', 'Can_List', 'Blacklisted', 
        'Curr_Listed?'
    ]
    spreadsheet = spreadsheet.reindex(columns=columns_order, fill_value=None)

    # Save the prepared DataFrame to a new CSV file
    spreadsheet.to_csv(output_file, index=False)

    print(f"Prepared data saved to {output_file}")

def update_master_db_without_gogolescrappingpy(master_db_path, update_csv_path, manual_Update_Ouput):
    # Check if Master_DB.csv exists
    if os.path.exists(master_db_path):
        # Read Master_DB.csv
        master_db = pd.read_csv(master_db_path)
    else:
        # Create an empty DataFrame if Master_DB.csv doesn't exist
        master_db = pd.DataFrame(columns=['Title', 'Sales Rank: 30 days avg.', 'Sales Rank: 90 days avg.', 'Buy Box: Current',
                                           'Buy Box: Stock', 'New: Current', 'Referral Fee %', 'New, 3rd Party FBM: 30 days avg.',
                                           'Lowest FBM Seller', 'Count of retrieved live offers: New, FBM', 'ASIN',
                                           'Product Codes: EAN', 'Product Codes: UPC', 'Parent ASIN', 'Brand',
                                           'Prime Eligible (Amazon offer)', 'Custom_SKU', 'Google_Search_Link', 'Match',
                                           'Amazon_link', 'Extraction_Link', 'Store', 'Price', 'Min Price', 'Max Price',
                                           'Amazon_List_price', 'Can_List', 'Blacklisted', 'Curr_Listed?'])

    # Read Update_CSV.csv
    update_csv = pd.read_csv(update_csv_path)

    # Counter for added items
    added_count = 0

    # Iterate through rows in Update_CSV and update Master_DB accordingly
    for index, row in update_csv.iterrows():
        asin = row['ASIN']
        black_list = row.get('Blacklisted')
        ean = row.get('Product Codes: EAN')
        upc = row["Product Codes: UPC"]

        # Find the row in Master_DB that matches the ASIN
        master_row_index = master_db.index[master_db['ASIN'] == asin]

        if not master_row_index.empty:
            # ASIN found in Master_DB, update Blacklist if provided
            if black_list is not None:
                master_db.loc[master_row_index, 'Blacklisted'] = black_list
        else:
            # ASIN not found in Master_DB, add a new row
            new_row = pd.DataFrame({'Title': [row['Title']], 'Sales Rank: 30 days avg.': [row['Sales Rank: 30 days avg.']],
                                    'Sales Rank: 90 days avg.': [row['Sales Rank: 90 days avg.']], 'Buy Box: Current': [row['Buy Box: Current']],
                                    'Buy Box: Stock': [row['Buy Box: Stock']], 'New: Current': [row['New: Current']],
                                    'Referral Fee %': [row['Referral Fee %']], 'New, 3rd Party FBM: 30 days avg.': [row['New, 3rd Party FBM: 30 days avg.']],
                                    'Lowest FBM Seller': [row['Lowest FBM Seller']], 'Count of retrieved live offers: New, FBM': [row['Count of retrieved live offers: New, FBM']],
                                    'ASIN': [asin], 'Product Codes: EAN': [ean], 'Product Codes: UPC': [upc],
                                    'Parent ASIN': [row['Parent ASIN']], 'Brand': [row['Brand']],
                                    'Prime Eligible (Amazon offer)': [row['Prime Eligible (Amazon offer)']],
                                    'Custom_SKU': [row['Custom_SKU']], 'Google_Search_Link': [row['Google_Search_Link']],
                                    'Match': [row['Match']], 'Amazon_link': [row['Amazon_link']],
                                    'Extraction_Link': [row['Extraction_Link']], 'Store': [row['Store']],
                                    'Price': [row['Price']], 'Min Price': [row['Min Price']], 'Max Price': [row['Max Price']],
                                    'Amazon_List_price': [row['Amazon_List_price']], 'Can_List': [row['Can_List']],
                                    'Blacklisted': [black_list], 'Curr_Listed?': [row['Curr_Listed?']]})
            master_db = pd.concat([master_db, new_row], ignore_index=True)
            added_count += 1

    # Save the updated Master_DB
    master_db.to_csv(manual_Update_Ouput, index=False)

    print(f"{added_count} items were added to Master_DB successfully.")



def upload_csv_via_ftp(hostname, username, password, local_file_path, remote_dir_path, remote_file_name, port=21):
    ftp = None
    try:
        ftp = FTP_TLS()
        ftp.set_debuglevel(2)
        
        # Connect and secure the control channel
        print("Connecting to FTP server...")
        ftp.connect(hostname, port)
        print("Securing control channel...")
        ftp.auth()  # Secure the control channel
        print("Logging in...")
        ftp.login(username, password)
        print("Logged in. Securing data channel...")
        ftp.prot_p()  # Secure the data channel
        
        print("Logged in. Setting passive mode and securing channel...")
        ftp.set_pasv(True)
        try:
            ftp.prot_p()  # Secure the data channel
        except Exception as prot_p_exception:
            print("prot_p() raised an exception:", prot_p_exception)
            print("Continuing despite the exception.")

        
        # Change to the appropriate directory on the server
        ftp.cwd(remote_dir_path)
        
        # Open the local CSV file
        with open(local_file_path, 'rb') as file:
            # Upload the CSV file to the server
            ftp.storbinary('STOR ' + remote_file_name, file)
        print("File uploaded successfully!")

    except Exception as e:
        print("An error occurred:", e)

    finally:
        # Close the FTP connection
        if ftp:
            ftp.quit()
            print("FTP connection closed.")