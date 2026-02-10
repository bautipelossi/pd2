import pandas as pd

df = pd.read_csv("mlb_games_ny_stadiums_2023.csv")

df["start_time_ny"] = pd.to_datetime(df["start_time_ny"], errors="coerce")
df["end_time_ny"] = pd.to_datetime(df["end_time_ny"], errors="coerce")

df_first_week = df[df["start_time_ny"].dt.day.between(1, 7)].copy()
df_first_week.sort_values("start_time_ny", inplace=True)

print(f"Partidos en la primera semana de cada mes: {len(df_first_week)}")
print(df_first_week[["venue", "start_time_ny", "end_time_ny"]].head())

df_first_week.to_csv("mlb_games_ny_first_week_2023.csv", index=False)
print("Archivo guardado: mlb_games_ny_first_week_2023.csv")
