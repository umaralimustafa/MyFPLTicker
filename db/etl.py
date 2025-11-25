"""
etl.py
Script to pull data from an external API and load it into MongoDB.
"""

import requests
from pymongo import MongoClient
import json
import os

# Example API endpoint
API_URL = "https://api.example.com/data"

# MongoDB connection parameters
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "fpl_db"
COLLECTION_NAME = "stats"

# Load sample data from file (for demonstration)
def load_sample_json():
    with open("sample.json", "r") as f:
        return json.load(f)

def fetch_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def load_data_to_mongodb(data):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)
    client.close()

def main():
    # For demonstration, load from sample.json
    data = load_sample_json()
    # For real API usage, uncomment below:
    # data = fetch_data()
    load_data_to_mongodb(data)

if __name__ == "__main__":
    main()
