import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path

# ===============================
# Rutas del proyecto
# ===============================

BASE_DIR = Path(__file__).resolve()
PROJECT_ROOT = BASE_DIR.parents[2]
DATA_DIR = PROJECT_ROOT / "datos" / "crudos"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "nyc_fhv_2023_sampled.csv"

# ===============================
# Configuración API Socrata
# ===============================

URL = "https://data.cityofnewyork.us/resource/u253-aew4.json"

COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "trip_miles",
    "base_passenger_fare",
    "tolls",
    "tips",
    "driver_pay"
]

LIMIT = 50000          # tamaño de chunk
DAYS_PER_MONTH = 7     # muestreo: primeros 7 días
SLEEP_TIME = 0.2       # para no matar el API

# ===============================
#  Descarga FHV 2023 (robusta)
# ===============================

def download_fhv_sample_2023():
    first_write = True

    print(" Descargando FHV 2023 (muestreo mensual por días)")

    for month in range(1, 13):
        month_start = datetime(2023, month, 1)
        print(f"\n Mes {month:02d} | muestreo {DAYS_PER_MONTH} días")

        for day in range(DAYS_PER_MONTH):
            start = month_start + timedelta(days=day)
            end = start + timedelta(days=1)

            offset = 0
            total_day = 0

            print(f"   Día {start.date()}")

            while True:
                params = {
                    "$limit": LIMIT,
                    "$offset": offset,
                    "$select": ",".join(COLUMNS),
                    "$where": (
                        f"pickup_datetime >= '{start.isoformat()}' "
                        f"AND pickup_datetime < '{end.isoformat()}'"
                    )
                }

                try:
                    r = requests.get(URL, params=params, timeout=30)
                    r.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"     Error API: {e}")
                    break

                data = r.json()

                if not data:
                    break

                df = pd.DataFrame(data)

                df.to_csv(
                    OUTPUT_FILE,
                    mode="a",
                    header=first_write,
                    index=False
                )

                first_write = False
                rows = len(df)
                total_day += rows
                offset += LIMIT

                print(f"     → {total_day} filas acumuladas")

                time.sleep(SLEEP_TIME)

    print(f"\nDataset FHV 2023 muestreado guardado en:\n{OUTPUT_FILE}")

# ===============================
#  Main
# ===============================

if __name__ == "__main__":
    download_fhv_sample_2023()
