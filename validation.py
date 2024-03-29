from jsonschema import validate, ValidationError
import json
import requests
from requests_aws4auth import AWS4Auth

LWA_CLIENT_ID = 'amzn1.application-oa2-client.4fb7f25cee5e4b4a8fe6d66d94e54e01'
LWA_CLIENT_SECRET = 'amzn1.oa2-cs.v1.228285a035066c8952b93ee4de8aa6a6e8cce2ef7cd28d91d750fad5f06486e9'
REFRESH_TOKEN = 'Atzr|IwEBICuQWBd9GDJxHRnuz0mJiWvKI5AKvylvu1gfGcNkixJQ7ROQvShjEajiZI6GpLaW05yC9ieiyy-BkNoXFkMcl_cB2mEkaYncs5F29GqWPzvo6WhX8Iri4xhwbm3Yrta822Re17NatiwHmuu0bxsbDncs-NsUFf4g_M07EUqjCjKsQO-_9iuJGAoNPUQWe5FlHGuPQ8Avd-XsrAX5rm_8WtLaqNkb_OJUBSYg-xE2HBwIJdeTx0_TQjGH7V7OiaeDuJKcSQLU-vmyNeknQrobcX2NJBk1CqyowecRDlK1qIr4t1y1i1BFxnzrRzqak55pdog'
ACCESS_KEY = 'ASIA5FTZASTVMPALUI7X'
SECRET_KEY = 'vah/qPQ/rhV/PUA1ArsvwKaD0UdjZxubacg6p5m+'
ROLE_ARN = 'arn:aws:iam::905418151146:role/SellingPartnerAPI_Leap'  # The role ARN for SP-API access
marketplace_id = 'ATVPDKIKX0DER'  # Example marketplace ID for the US
product_type = 'SHOES'

# Function to get LWA access token
def get_lwa_access_token(client_id, client_secret, refresh_token):
    url = 'https://api.amazon.com/auth/o2/token'
    data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token
    }
    response = requests.post(url, data=data)
    return response.json()['access_token']

access_token = get_lwa_access_token(LWA_CLIENT_ID, LWA_CLIENT_SECRET, REFRESH_TOKEN)

# This is a mock function, replace it with the actual method to retrieve the schema
def get_product_type_definition(access_token, marketplace_id, product_type):
    url = f'https://sellingpartnerapi-na.amazon.com/definitions/2020-09-01/productTypes/{product_type}'
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    params = {
        'marketplaceIds': marketplace_id
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# Mock payload - replace this with your actual payload
payload = {
    "marketplaceIds": [marketplace_id],
    "productType": "SHOES",
    "requirements": "LISTING",
    "attributes": {
        "item_name": 'New Balance Womens Fresh Foam Roav V1 Sneaker, Iodine Violet/Peony, 6',
        "brand_name": 'New Balance',
        "external_product_id": 197375483467,
        "external_product_id_type": "UPC",
        "quantity": 1,
        "condition_type": "new",
        "price": {
            "currency": "USD",
            "amount": 150
                },
        "fulfillment_latency": "1"
         # Add additional attributes according to the product type's schema
         },
         "mode": "CREATE" 
 }


# Validate the payload against the schema
schema = get_product_type_definition(access_token, marketplace_id, product_type)

try:
    validate(instance=payload, schema=schema)
    print("Payload is valid according to the schema.")
except ValidationError as e:
    print("Payload validation failed:", e)

# Proceed with the rest of your script if validation passes
