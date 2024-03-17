from faker import Faker
from pymongo import MongoClient
from task1_mongo_connector import connect_to_mongodb

# Function to connect to MongoDB
db = connect_to_mongodb()

# Function to insert multiple comments into MongoDB
def insert_comments(db, comments):
    try:
        db.comments.insert_many(comments)
        print("Success: Multiple comments inserted.")
    except Exception as e:
        print("Error in insertion:", e)

# Function to insert multiple movies into MongoDB
def insert_movies(db, movies):
    try:
        db.movies.insert_many(movies)
        print("Success: Multiple movies inserted.")
    except Exception as e:
        print("Error in insertion:", e)

# Function to insert multiple theaters into MongoDB
def insert_theaters(db, theaters):
    try:
        db.theaters.insert_many(theaters)
        print("Success: Multiple theaters inserted.")
    except Exception as e:
        print("Error in insertion:", e)

# Function to insert multiple users into MongoDB
def insert_users(db, users):
    try:
        db.users.insert_many(users)
        print("Success: Multiple users inserted.")
    except Exception as e:
        print("Error in insertion:", e)

# Instantiate Faker object
fake = Faker()

# Generate sample data for multiple comments
sample_comments = [
    {
        "name": fake.name(),
        "email": fake.email(),
        "text": fake.text(),
        "date": fake.date_time()
    }
    for _ in range(5)  # Generate 5 comments
]

# Generate sample data for multiple movies
sample_movies = [
    {
        "plot": fake.text(),
        "genres": [fake.word() for _ in range(3)],  # Generate 3 random genres
    }
    for _ in range(3)  # Generate 3 movies
]

# Generate sample data for multiple theaters
sample_theaters = [
    {
        "theaterId": fake.random_number(digits=4),
        # Generate random location data
        "location": {
            "address": {
                "street1": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "zipcode": fake.zipcode()
            },
            "geo": {
                "type": "Point",
                "coordinates": [float(fake.longitude()), float(fake.latitude())]
            }
        }
    }
    for _ in range(2)  # Generate 2 theaters
]

# Generate sample data for multiple users
sample_users = [
    {
        "name": fake.name(),
        "email": fake.email(),
    }
    for _ in range(3)  # Generate 3 users
]

# Insert generated data into respective collections
insert_comments(db, sample_comments)
insert_movies(db, sample_movies)
insert_theaters(db, sample_theaters)
insert_users(db, sample_users)
