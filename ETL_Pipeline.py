import requests
import psycopg2
import pandas as pd
from datetime import datetime

# Step 3: Extracting Data from OpenWeatherMap API
def extract_weather_data(api_url, api_key):
    """Extract data from the weather API."""
    try:
        response = requests.get(f"{api_url}&appid={api_key}")
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Step 4: Transforming Data
def transform_weather_data(data):
    """Transform raw weather data into a structured format."""
    try:
        transformed_data = {
            'date': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d'),
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'location': data['name'],
            'recorded_at': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
        return transformed_data
    except KeyError as e:
        print(f"Error transforming data: {e}")
        return None

# Step 5: Loading Data into PostgreSQL
def load_data_to_postgres(transformed_data, db_config):
    """Load transformed data into PostgreSQL."""
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Insert data into the table
        query = """
        INSERT INTO test_weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, list(transformed_data.values()))
        
        connection.commit()
        print("Data successfully loaded into PostgreSQL!")
    except psycopg2.Error as e:
        print(f"Error loading data to PostgreSQL: {e}")
    finally:
        cursor.close()
        connection.close()

# Error Handling & Monitoring
def main():
    # API Configuration
    API_KEY = 'YOUR_API_KEY'
    API_URL = 'http://api.openweathermap.org/data/2.5/weather?q=London'

    # Database Configuration
    db_config = {
        'dbname': 'ml_model_data',
        'user': 'postgres',
        'password': 'YOUR_DB_PASSWORD',
        'host': 'localhost',
        'port': '5432'
    }

    print("Starting ETL pipeline...")

    # Extract
    raw_data = extract_weather_data(API_URL, API_KEY)
    if not raw_data:
        return

    # Transform
    structured_data = transform_weather_data(raw_data)
    if not structured_data:
        return

    # Load
    load_data_to_postgres(structured_data, db_config)

    print("ETL pipeline completed successfully!")

# Execute the ETL process
if __name__ == "__main__":
    main()
