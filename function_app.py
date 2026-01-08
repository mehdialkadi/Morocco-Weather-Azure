import azure.functions as func
import os
import requests
import json
import logging
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Liste des villes marocaines
CITIES = [
    {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"name": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"name": "Marrakech", "lat": 31.6295, "lon": -7.9811},
    {"name": "Tanger", "lat": 35.7595, "lon": -5.8340},
    {"name": "Fes", "lat": 34.0331, "lon": -5.0003},
    {"name": "Agadir", "lat": 30.4278, "lon": -9.5981},
]

# Chaque début d'heure : seconde=0, minute=0
@app.function_name(name="weather_ingestion")
@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=True)
def weather_ingestion(myTimer: func.TimerRequest) -> None:
    logging.info("Démarrage de l'ingestion météo...")

    try:
        # 1) Authentification Managed Identity
        credential = DefaultAzureCredential()

        # 2) Récupération de la clé API depuis Key Vault
        key_vault_name = os.environ["KEY_VAULT_NAME"]
        vault_url = f"https://{key_vault_name}.vault.azure.net/"
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        api_key = secret_client.get_secret("OpenWeatherApiKey").value
        if not api_key:
            raise RuntimeError("OpenWeatherApiKey est vide ou introuvable dans Key Vault.")

        # 3) Connexion au Blob Storage (AzureWebJobsStorage)
        storage_conn = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn)
        container_name = "weather-raw"

        for city in CITIES:
            url = (
                "https://api.openweathermap.org/data/2.5/weather"
                f"?lat={city['lat']}&lon={city['lon']}&appid={api_key}&units=metric"
            )

            try:
                response = requests.get(url, timeout=20)
            except Exception as req_err:
                logging.error(f"❌ Requête échouée pour {city['name']}: {req_err}")
                continue

            if response.status_code != 200:
                logging.error(
                    f"❌ API OpenWeather erreur pour {city['name']} "
                    f"(status={response.status_code}): {response.text[:200]}"
                )
                continue

            weather_data = response.json()

            # Organisation: api-ingestion/Ville/Année/Mois/Jour/Heure-Minute
            now = datetime.utcnow()
            blob_name = f"api-ingestion/{city['name']}/{now.strftime('%Y/%m/%d/%H-%M')}_data.json"

            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(json.dumps(weather_data), overwrite=True)

            logging.info(f"✅ Sauvegardé : {blob_name}")

    except Exception as e:
        logging.error(f"🔥 Erreur globale dans weather_ingestion : {str(e)}", exc_info=True)
