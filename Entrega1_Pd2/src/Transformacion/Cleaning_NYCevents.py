import pandas as pd

df = pd.read_csv("NYC_SAPO_Events_ALL.csv")

df["start_date_time"] = pd.to_datetime(df["start_date_time"], errors="coerce")
df["end_date_time"] = pd.to_datetime(df["end_date_time"], errors="coerce")

df_first_week = df[df["start_date_time"].dt.day.between(1, 7)].copy()
df_first_week.sort_values("start_date_time", inplace=True)

print(f"Eventos en la primera semana de cada mes: {len(df_first_week)}")
print(df_first_week[["event_name", "start_date_time", "event_borough", "event_location"]].head())

df_first_week.to_csv("NYC_SAPO_Events_2023_first_week.csv", index=False)
print("Archivo guardado: NYC_SAPO_Events_2023_first_week.csv")