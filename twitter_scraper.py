import tweepy
import pandas as pd

# Set up API credentials (Bearer Token is sufficient)
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAO8XxQEAAAAAYZHd9AjRAZBA7vwK%2BKOsLsKYPfA%3DQf2JC8B5KYkTV02pAruR3kcj9HsaFxBL3bj4Rwgv5aBH5w3Sh1'

# Authenticate with Twitter API using Bearer Token (OAuth 2.0)
client = tweepy.Client(bearer_token)

# Fetch tweets based on a keyword (e.g., 'DataScience')
keyword = 'DataScience'
tweets = tweepy.Paginator(client.search_recent_tweets, query=keyword, tweet_fields=["author_id", "text"], max_results=100).flatten(limit=100)

# Create DataFrame from the fetched tweets
df = pd.DataFrame([[tweet.author_id, tweet.text] for tweet in tweets], columns=['User', 'Tweet'])

# Print the first few tweets
print(df.head())