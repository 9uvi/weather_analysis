import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OWM_API_KEY")
CITIES = [
    "Tokyo", "Delhi", "Shanghai", "São Paulo", "Mexico City", "Bucharest",
]

def fetch_weather_data():
    data = []
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url).json()
        data.append({
            "timestamp": datetime.now().isoformat(),
            "city": city,
            "temp": response["main"]["temp"],
            "humidity": response["main"]["humidity"],
            "wind_speed": response["wind"]["speed"]
        })
    df = pd.DataFrame(data)
    df.to_csv("weather_data.csv", index=False)
    return df


if __name__ == "__main__":
    fetch_weather_data()

    # Load data into Postgres
    # Connect to PostgreSQL
    engine = create_engine(f"postgresql://data_user:{os.getenv('POSTGRESS_PASSWORD')}@localhost:5432/weather_db")

    # Load CSV data
    df = pd.read_csv("weather_data.csv")
    df.to_sql("weather", engine, if_exists="append", index=False)