import requests
from bs4 import BeautifulSoup
import csv

def send_request():
    # URL of the Google Shopping page
    URL = 'https://www.google.com/shopping/product/15225287722088680527/offers?q=684835209830'
    proxies = {
        "https": "scraperapi.session_number=1:c003d52e137f9e4f19bf75317d5b377c@proxy-server.scraperapi.com:8001"
        }

    try:
        # Send an HTTP GET request to the URL
        response = requests.get(URL, proxies=proxies, verify=False)

        # Check if the request was successful (status code 200)
        with open('google_shopping_page.html', 'w', encoding='utf-8') as html_file:
                html_file.write(response.text)

        if response.status_code == 200 and 'Steve' in str(response.text):
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            element = soup.find(class_='BvQan sh-t__title sh-t__title-pdp translate-content')

            # Extract the text from the tag
            if element:
                text = element.text.strip()
                print(f"""
                    ProductName: {text}
                """)
            ##TODO Scrape Key information and save into file
            
        else:
            print(f"Request failed with status code {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


import time
for i in range(5):
    start_time = time.time()
    send_request()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Iteration {i + 1}: Time taken - {elapsed_time:.2f} seconds")