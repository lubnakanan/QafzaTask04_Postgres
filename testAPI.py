import requests

API_KEY = 'c872d3baeb70b4b22323919b67783c4f'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
CITY = 'London'

params = {
    'q': CITY,
    'appid': API_KEY,
    'units': 'metric'
}

response = requests.get(BASE_URL, params=params)
if response.status_code == 200:
    print("API Call Successful")
    print(response.json())
else:
    print("Failed to fetch data:", response.status_code)
