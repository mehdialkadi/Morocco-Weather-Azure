import azure.functions as func
import logging
import os
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# === Configuration Open-Meteo ===
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# === Liste des 20 villes du Dataset ===
CITIES = [
    {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"name": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"name": "Marrakech", "lat": 31.6295, "lon": -7.9811},
    {"name": "Fès", "lat": 34.0331, "lon": -5.0003},
    {"name": "Tanger", "lat": 35.7595, "lon": -5.8340},
    {"name": "Meknès", "lat": 34.2610, "lon": -6.5802},
    {"name": "Agadir", "lat": 30.4278, "lon": -9.5981},
    {"name": "Safi", "lat": 32.2994, "lon": -9.2372},
    {"name": "Beni Mellal", "lat": 32.3373, "lon": -6.3498},
    {"name": "Nador", "lat": 35.1681, "lon": -2.9335},
    {"name": "Mohammedia", "lat": 33.6874, "lon": -7.3820},
    {"name": "Tétouan", "lat": 35.5722, "lon": -5.3724},
    {"name": "El Jadida", "lat": 33.2316, "lon": -8.5007},
    {"name": "Oujda", "lat": 34.6867, "lon": -1.9114},
    {"name": "Ouarzazate", "lat": 30.9189, "lon": -6.8934},
    {"name": "Essaouira", "lat": 31.5085, "lon": -9.7595},
    {"name": "Tiznit", "lat": 29.6974, "lon": -9.7316},
    {"name": "Al Hoceima", "lat": 35.2517, "lon": -3.9370},
    {"name": "Laâyoune", "lat": 27.1510, "lon": -13.1990},
    {"name": "Dakhla", "lat": 23.6848, "lon": -15.9570},
]

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=True) 
def weather_ingestion_openmeteo(myTimer: func.TimerRequest) -> None:
    logging.info('Début de l\'ingestion horaire Open-Meteo...')

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": [c["lat"] for c in CITIES],
        "longitude": [c["lon"] for c in CITIES],
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation",
            "rain", "snowfall", "snow_depth", "wind_speed_10m", "wind_direction_10m",
            "wind_gusts_10m", "is_day", "dew_point_2m", "pressure_msl", "surface_pressure",
            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
            "shortwave_radiation", "direct_radiation"
        ],
        "daily": ["sunrise", "sunset", "precipitation_hours"],
        "timezone": "auto",
        "forecast_days": 1 # On ne prend que le jour actuel pour l'ingestion continue
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        all_data = []

        for i, response in enumerate(responses):
            city_name = CITIES[i]["name"]
            
            # --- Extraction HOURLY ---
            hourly = response.Hourly()
            hourly_dates = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )

            df_h = pd.DataFrame({
                "datetime_utc": hourly_dates,
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
                "apparent_temperature": hourly.Variables(2).ValuesAsNumpy(),
                "precipitation": hourly.Variables(3).ValuesAsNumpy(),
                "rain": hourly.Variables(4).ValuesAsNumpy(),
                "snowfall": hourly.Variables(5).ValuesAsNumpy(),
                "snow_depth": hourly.Variables(6).ValuesAsNumpy(),
                "wind_speed_10m": hourly.Variables(7).ValuesAsNumpy(),
                "wind_direction_10m": hourly.Variables(8).ValuesAsNumpy(),
                "wind_gusts_10m": hourly.Variables(9).ValuesAsNumpy(),
                "is_day": hourly.Variables(10).ValuesAsNumpy(),
                "dew_point_2m": hourly.Variables(11).ValuesAsNumpy(),
                "pressure_msl": hourly.Variables(12).ValuesAsNumpy(),
                "surface_pressure": hourly.Variables(13).ValuesAsNumpy(),
                "cloud_cover": hourly.Variables(14).ValuesAsNumpy(),
                "cloud_cover_low": hourly.Variables(15).ValuesAsNumpy(),
                "cloud_cover_mid": hourly.Variables(16).ValuesAsNumpy(),
                "cloud_cover_high": hourly.Variables(17).ValuesAsNumpy(),
                "shortwave_radiation": hourly.Variables(18).ValuesAsNumpy(),
                "direct_radiation": hourly.Variables(19).ValuesAsNumpy(),
                "city": city_name
            })
            
            # Note: Pour rester simple, on sauvegarde les données horaires. 
            # Les données Daily (sunrise/sunset) peuvent être jointes plus tard en SQL.
            all_data.append(df_h)

        # Fusion et export
        final_df = pd.concat(all_data, ignore_index=True)
        csv_data = final_df.to_csv(index=False)

        # --- Upload vers Azure Blob Storage ---
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        now = datetime.utcnow()
        blob_name = f"ingestion-v2/{now.year}/{now.month:02d}/{now.day:02d}/weather_{now.strftime('%H%M')}.csv"
        
        blob_client = blob_service_client.get_blob_client(container="weather-raw", blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        
        logging.info(f"✅ Ingestion réussie : {blob_name} ({len(final_df)} lignes)")

    except Exception as e:
        logging.error(f"❌ Erreur lors de l'ingestion : {str(e)}")