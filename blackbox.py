import requests
import json
from pymongo import MongoClient
import pandas as pd

# Load Excel data
excel_data = pd.read_excel(r'C:\Users\Desk0012\Downloads\Alkem_splited_client_data.xlsx',sheet_name='Sheet_17')

# Function to fetch URLs from API
def fetch_urls(search_key, index):
    url = "https://api.blackbox.ai/api/check"
    payload = json.dumps({
        "query": search_key,
        "messages": [
            {
                "id": "zbzvNkuxyy",
                "content": search_key,
                "role": "user"
            }
        ],
        "index": index
    })
    headers = {
        'Content-Type': 'application/json',
        'Cookie': 'sessionId=f1b66f2c-e64f-4a05-bb40-8d21bc8360aa'
    }
    
    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")  # Update connection string if needed
db = client['Alkem']  # Database name
collection = db['fullScrap']  # Collection name

# Process each row in the Excel data
for i, row in excel_data.iterrows():
    record_id = row.get("HCP ")
    name = row.get("HCP NAME (AS PER CARD) ")
    city = row.get("PLACE")
    indexes = [1,10, 20]
    
    if pd.isna(name) or pd.isna(city):  # Skip rows with missing data
        print(f"Row {i} skipped due to missing name or city.")
        continue
    
    search_key = f"Dr. {name} Gynic in {city}"
    
    try:
        for idx in indexes:
            # Fetch data from API
            data = fetch_urls(search_key, idx)
            
            # Process and store organic results in MongoDB
            if 'results' in data and 'organic' in data['results']:
                organic_results = data['results']['organic']
                for result in organic_results:
                    result['record_id'] = record_id  # Add record ID
                    collection.insert_one(result)  # Insert result into MongoDB
                print(f"Data stored successfully for row {i}, index {idx}.")
            else:
                print(f"No organic results found for row {i}, index {idx}.")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for row {i}, index {idx}: {e}")
    except Exception as e:
        print(f"An error occurred for row {i}: {e}")
