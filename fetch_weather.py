import os
import requests
import shutil
import pathlib
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

CWD = pathlib.Path(__file__).parent.resolve()
ARCHIVE_PATH = CWD / "archive"

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

def archive(file_name):
    os.rename(file_name, ARCHIVE_PATH / f"{file_name[:-4]}_{datetime.now().strftime('%d%m_%H%M')}.csv")

if __name__ == "__main__":
    fetch_weather_data()

    # Load data into Postgres
    # Connect to PostgreSQL
    engine = create_engine(f"postgresql://data_user:{os.getenv('POSTGRESS_PASSWORD')}@localhost:5432/weather_db")

    # Load CSV data
    df = pd.read_csv("weather_data.csv")
    df.to_sql("weather", engine, if_exists="append", index=False)

    # Archive .csv file
    archive("weather_data.csv")