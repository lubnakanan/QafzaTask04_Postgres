import time
import tweepy
import pandas as pd
from tweepy.errors import TooManyRequests

# Your API keys and Bearer Token
api_key = "lYskRO9jQOZVJTOACogq8qNvb"
api_key_secret = "hfpaZljiNN2U1PFKYYRpULltpc7ZOHrYBc2cK50ypVpd61cPQp"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAO8XxQEAAAAAYZHd9AjRAZBA7vwK%2BKOsLsKYPfA%3DQf2JC8B5KYkTV02pAruR3kcj9HsaFxBL3bj4Rwgv5aBH5w3Sh1"

# Set up Tweepy client
client = tweepy.Client(bearer_token=bearer_token)

# Define search query
query = "Python"  # Example query, change as needed

# Fetch tweets with rate limit handling
tweets = []
for tweet in tweepy.Paginator(client.search_recent_tweets, query=query, tweet_fields=["author_id", "text"]).flatten(limit=100):
    try:
        tweets.append([tweet.author_id, tweet.text])
    except TooManyRequests as e:
        print("Rate limit exceeded. Waiting for 15 minutes...")
        time.sleep(15 * 60)  # Wait for 15 minutes before retrying
        continue

# Create DataFrame
df = pd.DataFrame(tweets, columns=['User', 'Tweet'])

# Assuming you have already connected to PostgreSQL and set up the table
import psycopg2

# Set up connection parameters for PostgreSQL
conn = psycopg2.connect(
    dbname="ml_model_data",  # Database name
    user="postgres",         # Your PostgreSQL username
    password="EngLubna",     # Your PostgreSQL password
    host="localhost",        # or your Docker container's IP
    port="5432"              # Default PostgreSQL port
)

cursor = conn.cursor()

# Insert tweets into the table
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO twitter_data (user_id, tweet)
        VALUES (%s, %s)
    """, (row['User'], row['Tweet']))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Tweets have been successfully inserted into the database.")
