from pymongo import MongoClient

def connect_to_mongodb():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    # Create or access the desired database
    db = client["Assgn"]
    return db

# Test connection
db = connect_to_mongodb()
print("Connected to MongoDB successfully!")
