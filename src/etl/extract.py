from pymongo import MongoClient
import pandas as pd


def _mongo_connect(host, port, username, password, db):
    if username and password:
        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{db}"
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


# Connect to MongoDB
db = _mongo_connect(
    host="localhost", port=27017, username=None, password=None, db="tweets"
)

# Make a query to the specific DB and Collection
cursor = db["covid2"].find(projection={"_id": 0, "created_at": 1})

# Expand the cursor and construct the DataFrame
df = pd.DataFrame(list(cursor))


if __name__ == "__main__":
    print(df.head(10))
