#  Install the Python Requests library:
# `pip install requests`
import requests
from bs4 import BeautifulSoup

def send_request():
    response = requests.get(
        url='https://app.scrapingbee.com/api/v1/',
        params={
            'api_key': '7WX8OW6NE4303BOGNO05RWHKP0COKSO0ZXZDF4Y0FKBZ5G7HDS5276Z1MCV9IU2EFRQC14OCU4AR7VI7',
            'url': 'https://www.google.com/shopping/product/15225287722088680527/offers?q=684835209830',
            'custom_google':True
        },
        
    )

    with open('google_shopping_page_scrapingbee.html', 'w', encoding='utf-8') as html_file:
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

    print('Response HTTP Status Code: ', response.status_code)
    # print('Response HTTP Response Body: ', response.content)
import time
for i in range(5):
    start_time = time.time()
    send_request()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Iteration {i + 1}: Time taken - {elapsed_time:.2f} seconds")