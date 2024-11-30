import tweepy
import pandas as pd
import time
import psycopg2
from psycopg2 import sql

# Set up Twitter API credentials
api_key = "kQrqMoOrZfZV5g1TfTlSXdxty"
api_secret = "RAlqvuQZnFSX0mVMdr0VcIUNeDbXgduze5nIDVoakKEz8XXvUF"
access_token = "1213454150031167489-hLJGHxHFt8qykDCnzWlXs2B0H70saR"
access_token_secret = "xn0l6duxa1gYyS29WZ6xyMsmxTCrutzIzgOcyWm0hssYr"

# Initialize the Tweepy client with OAuth 1.0a (API Key & Secret)
auth = tweepy.OAuth1UserHandler(
    consumer_key=api_key, 
    consumer_secret=api_secret, 
    access_token=access_token, 
    access_token_secret=access_token_secret
)
api = tweepy.API(auth)

# Set up your query and parameters for tweet scraping
query = "Python"  # Replace with your desired query
max_results = 100  # Adjust the number of tweets to scrape

# Function to handle the scraping of tweets
def scrape_tweets():
    tweets = []
    try:
        # Fetch tweets from the API (searching for tweets containing the query)
        for tweet in tweepy.Cursor(api.search_tweets, q=query, lang="en", result_type="recent", tweet_mode="extended").items(max_results):
            tweets.append(tweet)  # Add the fetched tweets to the list

    except tweepy.TweepError as e:
        print(f"Error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return tweets

# Function to save the tweets into a DataFrame
def create_dataframe(tweets):
    df = pd.DataFrame([[tweet.user.id, tweet.full_text] for tweet in tweets], columns=['User', 'Tweet'])
    return df

# Function to insert the tweets into PostgreSQL
def insert_tweets_to_db(df):
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(dbname="ml_model_data", user="postgres", password="EngLubna", host="localhost", port="5432")
        cursor = conn.cursor()

        # Insert tweets into the twitter_data table
        for _, row in df.iterrows():
            cursor.execute(
                sql.SQL("INSERT INTO twitter_data (user_id, tweet) VALUES (%s, %s)"),
                [row['User'], row['Tweet']]
            )

        # Commit the transaction
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()
        print("Tweets inserted successfully into the database.")

    except Exception as e:
        print(f"Error while inserting tweets into database: {e}")

# Main function to execute the workflow
def main():
    print("Starting tweet scraping...")
    tweets = scrape_tweets()
    print(f"Fetched {len(tweets)} tweets.")
    
    if tweets:
        df = create_dataframe(tweets)
        insert_tweets_to_db(df)
    else:
        print("No tweets to insert.")

if __name__ == "__main__":
    main()
