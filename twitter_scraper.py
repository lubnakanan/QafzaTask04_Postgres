import tweepy
import pandas as pd
import time
import psycopg2
from psycopg2 import sql

# Set up Twitter API credentials
bearer_token = "AAAAAAAAAAAAAAAAAAAAAO8XxQEAAAAAY0T3iusuQ48s8T%2FRcfzd%2Bf5JySk%3Ds0BH9ccxmQbVfmaoceRzi7Ou1jEZSqbOSs8mmWWb6ym9UpwaLM"

# Initialize the Tweepy client
client = tweepy.Client(bearer_token=bearer_token)

# Function to verify the connection to the Twitter API
def verify_authentication():
    try:
        user = client.get_me()  # Get the authenticated user's info
        print(f"Authenticated as {user.data['username']}")
    except tweepy.errors.Unauthorized as e:
        print(f"Authorization failed: {e}")
        exit(1)  # Exit the script if authentication fails

# Set up your query and parameters for tweet scraping
query = "Python"  # Replace with your desired query
max_results = 100  # Adjust the number of tweets to scrape

# Function to handle the scraping of tweets
def scrape_tweets():
    tweets = []
    next_token = None
    
    while True:
        try:
            # Fetch tweets from the API
            response = client.search_recent_tweets(query=query, max_results=max_results, next_token=next_token)
            tweets.extend(response.data)  # Add the fetched tweets to the list
            
            # Check if there are more tweets to fetch
            next_token = response.meta.get('next_token')
            
            # If no more tweets, break the loop
            if not next_token:
                break

        except tweepy.errors.TooManyRequests as e:
            print("Rate limit exceeded, waiting to retry...")
            time.sleep(15 * 60)  # Wait for 15 minutes before retrying
        except Exception as e:
            print(f"An error occurred: {e}")
            break  # Exit if any other error occurs

    return tweets

# Function to save the tweets into a DataFrame
def create_dataframe(tweets):
    df = pd.DataFrame([[tweet.author_id, tweet.text] for tweet in tweets], columns=['User', 'Tweet'])
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
    # First, verify authentication
    verify_authentication()

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
