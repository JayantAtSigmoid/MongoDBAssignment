from task1_mongo_connector import connect_to_mongodb

# Function to connect to MongoDB
db = connect_to_mongodb()

# Function to find the top 10 cities with the maximum number of theaters
def top_10_cities():
    pipeline = [
        {"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(db.theaters.aggregate(pipeline))
    return result

# Function to preprocess coordinates
def preprocess_coordinates(coordinates):
    return [float(coord) for coord in coordinates]

# Function to find the top 10 theaters nearby the given coordinates
def top_10_theater_nearby(longitude, latitude):
    # Preprocess coordinates
    for theater in db.theaters.find():
        coordinates = theater["location"]["geo"]["coordinates"]
        processed_coordinates = preprocess_coordinates(coordinates)
        theater["location"]["geo"]["coordinates"] = processed_coordinates
        db.theaters.update_one(
            {"_id": theater["_id"]},
            {"$set": {"location.geo.coordinates": processed_coordinates}}
        )
    
    # Create 2dsphere index on the 'location.geo.coordinates' field  
    db.theaters.create_index([("location.geo.coordinates", "2dsphere")])
    
    # Define the query to find theaters near the given coordinates
    query = {
        "location.geo.coordinates": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
            }
        }
    }

    # Execute the query to find theaters nearby
    result = db.theaters.find(query).limit(10)

    return list(result)

def main():
    print("\n")
    
    top_cities = top_10_cities()
    
    print("1 Top 10 cities with maximum number of theaters: \n")
    for index, city_data in enumerate(top_cities, start=1):
        city_name = city_data['_id']
        theater_count = city_data['count']
        print(f"{index}. {city_name}: {theater_count} theaters")
        
    top_nearby_theater = top_10_theater_nearby(130, 40)
    
    print("\n")

    print("2 Top 10 theaters nearby the given coordinates (130,40): \n")
    for index, theater in enumerate(top_nearby_theater, start=1):
        print(f"{index}. {theater['theaterId']} - {theater['location']['address']['street1']}, {theater['location']['address']['city']}, {theater['location']['address']['state']}")

if __name__ == "__main__":
    main()
