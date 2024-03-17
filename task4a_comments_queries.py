from task1_mongo_connector import connect_to_mongodb
from datetime import datetime


# Function to connect to MongoDB
db = connect_to_mongodb()

# Function to find the top 10 users who made the maximum number of comments
def top_10_users_with_most_comments(db):
    pipeline = [
        {"$group": {"_id": "$name", "total_comments": {"$sum": 1}}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

# Function to find the top 10 movies with the most comments
def top_movies_with_most_comments(db):
    pipeline = [
        {"$group": {"_id": "$movie_id", "total_comments": {"$sum": 1}}},
        {"$lookup": {"from": "movies", "localField": "_id", "foreignField": "_id", "as": "movie"}},
        {"$unwind": "$movie"},
        {"$project": {"title": "$movie.title", "total_comments": 1}},
        {"$sort": {"total_comments": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

# Function to find the total number of comments created each month in a given year
def comments_per_month_in_year(db, year):
    pipeline = [
        {"$match": {"date": {"$gte": datetime(year, 1, 1), "$lt": datetime(year + 1, 1, 1)}}},
        {"$group": {"_id": {"$month": "$date"}, "total_comments": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

def main():
    db = connect_to_mongodb()

    print("\n")

    # Call functions for the questions
    top_users = top_10_users_with_most_comments(db)
    print("Top 10 users with most comments:" + "\n")
    for user in top_users:
        print(user["_id"], "-", user["total_comments"], "comments")

    print("\n")
    top_movies = top_movies_with_most_comments(db)
    print("Top 10 movies with most comments: " + "\n")
    for doc in top_movies:
        print(f"{doc['title']} : {doc['total_comments']} comments")

    print("\n")
    year = 1996
    comments_per_month = comments_per_month_in_year(db, year)
    print(f"\nComments per month in {year}:" + "\n")
    if len(comments_per_month) == 0:
        print(f"No comments found for the year {year}")
    else:
        for doc in comments_per_month:
            print(f"{doc['_id']} : {doc['total_comments']} comments")

if __name__ == "__main__":
    main()
