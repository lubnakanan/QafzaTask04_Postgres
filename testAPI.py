import requests
import csv
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

    # Write data to CSV file
    with open('/path/to/output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['date', 'temperature', 'humidity', 'wind_speed', 'location', 'recorded_at'])
        writer.writerow([timestamp.date(), temperature, humidity, wind_speed, location, timestamp])
    print("Data written to CSV.")
else:
    print(f"API Call Failed with status code: {response.status_code}")
