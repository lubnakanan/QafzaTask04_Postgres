import requests
import psycopg2
from datetime import datetime

# Your API key and endpoint
api_key = "c872d3baeb70b4b22323919b67783c4f"
endpoint = "https://api.openweathermap.org/data/2.5/weather"
city = "London,GB"  

# Making the API request
url = f"{endpoint}?q={city}&APPID={api_key}&units=metric"
response = requests.get(url)

if response.status_code == 200:
    print("API Call Successful")
    data = response.json()

    # Extract required data from the response
    temperature = data['main']['temp']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    description = data['weather'][0]['description']
    location = data['name']
    timestamp = datetime.now()

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="ml_model_data",
        user="postgres",
        password="yourpassword"
    )
    cursor = conn.cursor()

    # Insert data into PostgreSQL table
    insert_query = """
    INSERT INTO test_weather_data (date, temperature, humidity, wind_speed, location, recorded_at)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (timestamp.date(), temperature, humidity, wind_speed, location, timestamp))
    conn.commit()
    print("Data inserted successfully into the database.")
    cursor.close()
    conn.close()
else:
    print(f"API Call Failed with status code: {response.status_code}")
