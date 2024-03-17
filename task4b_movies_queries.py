from task1_mongo_connector import connect_to_mongodb
from datetime import datetime


# Function to connect to MongoDB
db = connect_to_mongodb()

# b. i
# Function to find the top `N` movies with the highest IMDB rating
def top_movies_highest_imdb_rating(db, N):
    pipeline = [
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N},
        {"$project": {"title": 1}}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` movies with the highest IMDB rating in a given year
def top_movies_highest_imdb_rating_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` movies with the highest IMDB rating and number of votes > 1000
def top_movies_highest_imdb_rating_votes(db, N):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": 1000}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` movies with title matching a given pattern sorted by highest tomatoes ratings
def top_movies_title_matching_pattern(db, N, pattern):
    pipeline = [
        {"$match": {"title": {"$regex": pattern, "$options": "i"}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


#b. ii
# Function to find the top `N` directors who created the maximum number of movies
def top_directors_most_movies(db, N):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` directors who created the maximum number of movies in a given year
def top_directors_most_movies_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` directors who created the maximum number of movies for a given genre
def top_directors_most_movies_genre(db, N, genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# b(iii)
# Function to find the top `N` actors who starred in the maximum number of movies
def top_actors_most_movies(db, N):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` actors who starred in the maximum number of movies in a given year
def top_actors_most_movies_year(db, N, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

# Function to find the top `N` actors who starred in the maximum number of movies for a given genre
def top_actors_most_movies_genre(db, N, genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
        {"$sort": {"total_movies": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)

#b (iv)
# Function to find the top `N` movies for each genre with the highest IMDB rating
# Function to find top N movies for each genre with the highest IMDB rating
def top_movies_by_genre_with_highest_imdb_rating(db, N):
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "top_movies": {"$push": "$$ROOT"}}},
        {"$project": {"genre": "$_id", "top_movies": {"$slice": ["$top_movies", N]}}},
        {"$unwind": "$top_movies"},
        {"$sort": {"genre": 1, "top_movies.imdb.rating": -1}},  # Sort by genre and then by IMDb rating
        {"$group": {"_id": "$genre", "movies": {"$push": "$top_movies"}}}
    ]
    result = db.movies.aggregate(pipeline)
    return list(result)


def main():
    db = connect_to_mongodb()

    print("\n")

    # Call functions for the movie questions
    print("\n b(i) \n")
    print("1 Top 5 movies by imdb rating: \n")
    for doc in top_movies_highest_imdb_rating(db, 5):
        print(doc['title'])
    print("\n")

    N = 5
    year = 1996
    top_movies_imdb_rating_year = top_movies_highest_imdb_rating_year(db, N, year)
    print(f"\n2 Top {N} movies with the highest IMDB rating in {year}: \n")
    for movie in top_movies_imdb_rating_year:
        print(movie["title"], "-", movie["imdb"]["rating"])

    top_movies_imdb_votes = top_movies_highest_imdb_rating_votes(db, N)
    print(f"\n3 Top {N} movies with the highest IMDB rating and number of votes > 1000: \n")
    for movie in top_movies_imdb_votes:
        print(movie["title"], "-", movie["imdb"]["rating"], "(Votes:", movie["imdb"]["votes"], ")")

    pattern = "Avengers"
    top_movies_title_pattern = top_movies_title_matching_pattern(db, N, pattern)
    print(f"\n4 Top {N} movies with title matching pattern '{pattern}' sorted by highest tomatoes ratings:")
    for movie in top_movies_title_pattern:
        print(movie["title"], "-", movie["tomatoes"]["viewer"]["rating"])

    # Call functions for the director questions
    print("\n b(ii) \n")

    N = 5
    top_directors_most = top_directors_most_movies(db, N)
    print(f"1 Top {N} directors who created the maximum number of movies:")
    for director in top_directors_most:
        print(director["_id"], "-", director["total_movies"], "movies")

    year = 1996
    top_directors_year = top_directors_most_movies_year(db, N, year)
    print(f"\n2 Top {N} directors who created the maximum number of movies in {year}:")
    for director in top_directors_year:
        print(director["_id"], "-", director["total_movies"], "movies")

    genre = "Action"
    top_directors_genre = top_directors_most_movies_genre(db, N, genre)
    print(f"\n3 Top {N} directors who created the maximum number of movies for genre '{genre}':")
    for director in top_directors_genre:
        print(director["_id"], "-", director["total_movies"], "movies")

    # Call functions for the actor questions
    print("\n b(iii) \n")
    N = 5
    top_actors_most = top_actors_most_movies(db, N)
    print(f"1 Top {N} actors who starred in the maximum number of movies:")
    for actor in top_actors_most:
        print(actor["_id"], "-", actor["total_movies"], "movies")

    year = 1996
    top_actors_year = top_actors_most_movies_year(db, N, year)
    print(f"\n2 Top {N} actors who starred in the maximum number of movies in {year}:")
    for actor in top_actors_year:
        print(actor["_id"], "-", actor["total_movies"], "movies")

    genre = "Action"
    top_actors_genre = top_actors_most_movies_genre(db, N, genre)
    print(f"\n3 Top {N} actors who starred in the maximum number of movies for genre '{genre}':")
    for actor in top_actors_genre:
        print(actor["_id"], "-", actor["total_movies"], "movies")


    # Call function for movie question
    print("\n b(iv) \n")
    print("1 Top 3 movies by genre with highest IMDb rating:")
    for genre_data in top_movies_by_genre_with_highest_imdb_rating(db, 3):
        genre = genre_data['_id']
        print(f"\nGenre: {genre}")
        movies = genre_data['movies']
        for movie in movies:
            print(f"{movie['title']} : {movie['imdb']['rating']}")

if __name__ == "__main__":
    main()
