import pandas as pd
from pymongo import MongoClient

# Define the MongoDB connection parameters
mongo_params = {
    'host': 'localhost',  # MongoDB host
    'port': 27017,        # MongoDB port
    'database_name': 'loop_db',  # Replace with your database name
}

# Paths to CSV files
store_activity_csv = '/home/nlblr203/Downloads/store status.csv'
business_hours_csv = '/home/nlblr203/Downloads/Menu hours.csv'
timezones_csv = '/home/nlblr203/Downloads/store-timezone.csv'

# Function to load CSV data into a MongoDB collection
def load_csv_to_mongodb(csv_path, collection_name, db):
    df = pd.read_csv(csv_path)
    records = df.to_dict(orient='records')
    db[collection_name].insert_many(records)

try:
    # Connect to MongoDB
    client = MongoClient(mongo_params['host'], mongo_params['port'])
    db = client[mongo_params['database_name']]

    # Ingest data from CSV files into MongoDB collections
    load_csv_to_mongodb(store_activity_csv, 'store_status', db)
    load_csv_to_mongodb(business_hours_csv, 'business_hours', db)
    load_csv_to_mongodb(timezones_csv, 'timezone', db)

    print("Data ingestion completed.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if 'client' in locals():
        client.close()
