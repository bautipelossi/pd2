import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import itertools
import geopandas as gpd
import folium
from folium.plugins import TimestampedGeoJson
from sklearn.cluster import KMeans
import numpy as np

# --------------------------------------------------
# RUTAS
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "Entrega1_Pd2" / "datos" / "limpios"
OUT_DIR = Path(__file__).resolve().parents[1] / "Visualizacion" / "Patrones_Demanda"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# 1. CARGA DE DATOS
# --------------------------------------------------
def load_data(DATA_DIR):
    df = pd.read_parquet(DATA_DIR / "nyc_taxi_clean.parquet")
    return df


# --------------------------------------------------
# 2. VARIABLES TEMPORALES
# --------------------------------------------------
def add_time_variables(df):
    df = df.copy()
    df["hora"] = df["pickup_hour"]
    df["dia_semana"] = df["pickup_weekday"]
    df["fecha"] = df["tpep_pickup_datetime"].dt.date
    return df


# --------------------------------------------------
# 3. DEMANDA DIARIA (BASE)
# --------------------------------------------------
def build_daily_demand(df):
    demanda_diaria = (
        df.groupby(["fecha", "pulocationid", "hora"])
        .size()
        .reset_index(name="demanda")
    )
    return demanda_diaria


# --------------------------------------------------
# 4. PATRÓN HORARIO (MEDIA)
# --------------------------------------------------
def build_hourly_pattern(demanda_diaria):
    demanda = (
        demanda_diaria
        .groupby(["pulocationid", "hora"])["demanda"]
        .mean()
        .reset_index()
    )
    return demanda


# --------------------------------------------------
# 5. CLASIFICACIÓN POR ZONA
# --------------------------------------------------
def classify_demand(demanda):
    thresholds = (
        demanda
        .groupby("pulocationid")["demanda"]
        .quantile([0.33, 0.66])
        .unstack()
        .rename(columns={0.33: "Q1", 0.66: "Q3"})
        .reset_index()
    )

    demanda = demanda.merge(thresholds, on="pulocationid")

    def clasificar(row):
        if row["demanda"] <= row["Q1"]:
            return "baja"
        elif row["demanda"] >= row["Q3"]:
            return "alta"
        else:
            return "media"

    demanda["nivel_demanda"] = demanda.apply(clasificar, axis=1)

    return demanda

# --------------------------------------------------
# 6. HEATMAP PRINCIPAL
# --------------------------------------------------
def plot_main_heatmap(demanda, OUT_DIR):
    pivot = demanda.pivot(
        index="pulocationid",
        columns="hora",
        values="demanda"
    )

    plt.figure(figsize=(12,8))
    sns.heatmap(pivot, cmap="coolwarm")
    plt.title("Patrón de demanda por zona y hora")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "heatmap_patrones.png")
    plt.close()


# --------------------------------------------------
# 7. CURVAS TOP ZONAS
# --------------------------------------------------
def plot_top_zones(demanda, OUT_DIR):
    plt.figure(figsize=(12,6))

    top_zonas = (
        demanda.groupby("pulocationid")["demanda"]
        .mean()
        .sort_values(ascending=False)
        .head(5)
        .index
    )

    for zona in top_zonas:
        curva = demanda[demanda["pulocationid"] == zona]
        plt.plot(curva["hora"], curva["demanda"], label=f"Zona {zona}")

    plt.legend()
    plt.title("Curvas de demanda (Top zonas)")
    plt.xlabel("Hora")
    plt.ylabel("Demanda media")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "curvas_demanda.png")
    plt.close()


# --------------------------------------------------
# 8. BOXPLOT HORARIO
# --------------------------------------------------
def plot_boxplot(demanda, OUT_DIR):
    plt.figure(figsize=(12,6))
    sns.boxplot(x="hora", y="demanda", data=demanda)
    plt.title("Distribución de demanda por hora")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "boxplot_demanda.png")
    plt.close()


# --------------------------------------------------
# 9. ANÁLISIS SEMANAL
# --------------------------------------------------
def build_weekly_demand(df):
    demanda_semana = (
        df.groupby(["dia_semana", "hora"])
        .size()
        .reset_index(name="demanda")
    )
    return demanda_semana


def plot_weekly_curves(demanda_semana, OUT_DIR):
    mapa_dias = {
        0: "Lunes", 1: "Martes", 2: "Miércoles",
        3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"
    }

    demanda_semana = demanda_semana.copy()
    demanda_semana["dia_semana"] = demanda_semana["dia_semana"].map(mapa_dias)

    orden_dias = [
        "Lunes", "Martes", "Miércoles",
        "Jueves", "Viernes", "Sábado", "Domingo"
    ]

    plt.figure(figsize=(12,6))

    for dia in orden_dias:
        subset = demanda_semana[demanda_semana["dia_semana"] == dia]
        plt.plot(subset["hora"], subset["demanda"], label=dia)

    plt.legend()
    plt.title("Demanda por hora según día de la semana")
    plt.xlabel("Hora")
    plt.ylabel("Nº viajes")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "curvas_dias_semana.png")
    plt.close()


def plot_weekly_heatmap(demanda_semana, OUT_DIR):
    mapa_dias = {
        0: "Lunes", 1: "Martes", 2: "Miércoles",
        3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"
    }

    demanda_semana = demanda_semana.copy()
    demanda_semana["dia_semana"] = demanda_semana["dia_semana"].map(mapa_dias)

    orden_dias = [
        "Lunes", "Martes", "Miércoles",
        "Jueves", "Viernes", "Sábado", "Domingo"
    ]

    pivot_semana = demanda_semana.pivot(
        index="dia_semana",
        columns="hora",
        values="demanda"
    ).reindex(orden_dias)

    plt.figure(figsize=(12,6))
    sns.heatmap(pivot_semana, cmap="coolwarm")
    plt.title("Demanda por día de la semana y hora")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "heatmap_dias_semana.png")
    plt.close()

# --------------------------------------------------
# UTIL: REJILLA COMPLETA
# --------------------------------------------------
def build_full_grid(demanda, zones):
    zonas_ids = zones["LocationID"].unique()
    horas = range(24)

    grid = pd.DataFrame(
        list(itertools.product(zonas_ids, horas)),
        columns=["pulocationid", "hora"]
    )

    full = grid.merge(
        demanda,
        on=["pulocationid", "hora"],
        how="left"
    )

    full["demanda"] = full["demanda"].fillna(0)

    return full


# --------------------------------------------------
# UTIL: LEYENDA
# --------------------------------------------------
def add_legend(mapa, title="Demanda"):
    legend_html = f"""
    <div style="
    position: fixed;
    bottom: 40px;
    right: 40px;
    width: 160px;
    height: 110px;
    z-index:9999;
    font-size:14px;
    background-color:white;
    padding:10px;
    border:2px solid grey;
    border-radius:8px;
    ">
    <b>{title}</b><br>
    <i style="background:#2ca25f;width:10px;height:10px;display:inline-block;"></i> Baja<br>
    <i style="background:#feb24c;width:10px;height:10px;display:inline-block;"></i> Media<br>
    <i style="background:#de2d26;width:10px;height:10px;display:inline-block;"></i> Alta
    </div>
    """
    mapa.get_root().html.add_child(folium.Element(legend_html))

def map_global(demanda, DATA_DIR, OUT_DIR):
    print("Generando mapa GLOBAL...")

    zones = gpd.read_file(DATA_DIR / "taxi_zones" / "taxi_zones.shp")
    zones = zones.to_crs(epsg=4326)
    zones["LocationID"] = zones["LocationID"].astype(int)

    demanda_full = build_full_grid(demanda, zones)

    # CUARTILES GLOBALES
    q1 = demanda_full["demanda"].quantile(0.33)
    q3 = demanda_full["demanda"].quantile(0.66)

    def nivel(v):
        if v <= q1: return "baja"
        if v >= q3: return "alta"
        return "media"

    demanda_full["nivel"] = demanda_full["demanda"].apply(nivel)

    color_map = {
        "baja": "#2ca25f",
        "media": "#feb24c",
        "alta": "#de2d26"
    }

    features = []

    for _, zona in zones.iterrows():
        subset = demanda_full[demanda_full["pulocationid"] == zona["LocationID"]]

        for _, row in subset.iterrows():
            features.append({
                "type": "Feature",
                "geometry": zona["geometry"].__geo_interface__,
                "properties": {
                    "times": [f"2023-01-01T{int(row['hora']):02d}:00:00Z"],
                    "style": {
                        "fillColor": color_map[row["nivel"]],
                        "color": "black",
                        "fillOpacity": 1,
                        "weight": 0.5
                    }
                }
            })

    geojson = {"type": "FeatureCollection", "features": features}

    mapa = folium.Map(location=[40.7128, -74.0060], zoom_start=11, tiles="CartoDB positron")

    TimestampedGeoJson(geojson, period="PT1H", duration="PT1H").add_to(mapa)

    add_legend(mapa, "Demanda global")

    mapa.save(OUT_DIR / "mapa_global.html")

def map_local(demanda, DATA_DIR, OUT_DIR):
    print("Generando mapa POR ZONA...")

    zones = gpd.read_file(DATA_DIR / "taxi_zones" / "taxi_zones.shp")
    zones = zones.to_crs(epsg=4326)
    zones["LocationID"] = zones["LocationID"].astype(int)

    demanda_full = build_full_grid(demanda, zones)

    # CUARTILES POR ZONA
    quantiles = (
        demanda_full
        .groupby("pulocationid")["demanda"]
        .quantile([0.33, 0.66])
        .unstack()
        .rename(columns={0.33: "q1", 0.66: "q3"})
        .reset_index()
    )

    demanda_local = demanda_full.merge(quantiles, on="pulocationid")

    def nivel(row):
        if row["demanda"] <= row["q1"]: return "baja"
        if row["demanda"] >= row["q3"]: return "alta"
        return "media"

    demanda_local["nivel"] = demanda_local.apply(nivel, axis=1)

    color_map = {
        "baja": "#2ca25f",
        "media": "#feb24c",
        "alta": "#de2d26"
    }

    features = []

    for _, zona in zones.iterrows():
        subset = demanda_local[demanda_local["pulocationid"] == zona["LocationID"]]

        for _, row in subset.iterrows():
            features.append({
                "type": "Feature",
                "geometry": zona["geometry"].__geo_interface__,
                "properties": {
                    "times": [f"2023-01-01T{int(row['hora']):02d}:00:00Z"],
                    "style": {
                        "fillColor": color_map[row["nivel"]],
                        "color": "black",
                        "fillOpacity": 1,
                        "weight": 0.5
                    }
                }
            })

    geojson = {"type": "FeatureCollection", "features": features}

    mapa = folium.Map(location=[40.7128, -74.0060], zoom_start=11, tiles="CartoDB positron")

    TimestampedGeoJson(geojson, period="PT1H", duration="PT1H").add_to(mapa)

    add_legend(mapa, "Demanda por zona")

    mapa.save(OUT_DIR / "mapa_por_zona.html")

def clustering_analysis(demanda, DATA_DIR, OUT_DIR, k=5):
    print("Iniciando clustering...")

    # --------------------------------------------------
    # MATRIZ
    # --------------------------------------------------
    matriz = demanda.pivot(
        index="pulocationid",
        columns="hora",
        values="demanda"
    ).fillna(0)

    # --------------------------------------------------
    # NORMALIZACIÓN
    # --------------------------------------------------
    X = np.log1p(matriz)

    X = X.sub(X.mean(axis=1), axis=0)
    X = X.div(X.std(axis=1), axis=0).fillna(0)

    # --------------------------------------------------
    # KMEANS
    # --------------------------------------------------
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    matriz["cluster"] = kmeans.fit_predict(X)

    # --------------------------------------------------
    # INTERPRETACIÓN CLUSTERS
    # --------------------------------------------------
    cluster_labels = {}

    for c in range(k):
        subset = matriz[matriz["cluster"] == c].drop(columns="cluster")
        media = subset.mean()

        pico = media.idxmax()

        if 7 <= pico <= 10:
            label = f"Mañana (pico {pico}h)"
        elif 17 <= pico <= 20:
            label = f"Tarde / commuting (pico {pico}h)"
        elif pico >= 21 or pico <= 3:
            label = f"Nocturno (pico {pico}h)"
        else:
            label = f"Mixto (pico {pico}h)"

        cluster_labels[c] = label

    # --------------------------------------------------
    # CURVAS
    # --------------------------------------------------
    plt.figure(figsize=(12,6))

    for c in range(k):
        subset = matriz[matriz["cluster"] == c].drop(columns="cluster")
        media = subset.mean()

        plt.plot(media.index, media.values, label=cluster_labels[c])

    plt.legend()
    plt.title("Patrones por cluster")
    plt.xlabel("Hora")
    plt.ylabel("Demanda normalizada")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "clusters_curvas.png")
    plt.close()

    # --------------------------------------------------
    # MAPA (BASE BLANCO/NEGRO + COLORES CLUSTER)
    # --------------------------------------------------
    print("Generando mapa de clusters...")

    zones = gpd.read_file(DATA_DIR / "taxi_zones" / "taxi_zones.shp")
    zones = zones.to_crs(epsg=4326)

    clusters_df = matriz["cluster"].reset_index()

    zones = zones.merge(
        clusters_df,
        left_on="LocationID",
        right_on="pulocationid",
        how="left"
    )

    # 🎨 COLORES ORIGINALES (los buenos)
    cluster_colors = {
        0: "#1f77b4",
        1: "#2ca02c",
        2: "#d62728",
        3: "#9467bd",
        4: "#ff7f0e"
    }

    mapa = folium.Map(
        location=[40.7128, -74.0060],
        zoom_start=11,
        tiles="CartoDB positron"  # 👈 mapa limpio
    )

    for _, row in zones.iterrows():
        if pd.isna(row["cluster"]):
            continue

        cluster = int(row["cluster"])
        label = cluster_labels[cluster]

        folium.GeoJson(
            row["geometry"],
            style_function=lambda x, c=cluster: {
                "fillColor": cluster_colors.get(c, "#999999"),
                "color": "black",
                "weight": 0.4,
                "fillOpacity": 0.7
            },
            tooltip=f"{row.get('zone','Zona')} ({row.get('borough','')})<br>{label}"
        ).add_to(mapa)

    # --------------------------------------------------
    # LEYENDA (CLUSTERS INTERPRETADOS)
    # --------------------------------------------------
    legend_html = """
    <div style="
    position: absolute;
    z-index: 9999;
    bottom: 40px;
    right: 40px;
    width: 260px;
    background-color:white;
    padding:10px;
    border:2px solid grey;
    border-radius:8px;
    font-size:14px;
    ">
    <b>Tipos de zona</b><br>
    """

    for c, label in cluster_labels.items():
        color = cluster_colors.get(c, "#999999")
        legend_html += f"""
        <i style="background:{color};width:10px;height:10px;display:inline-block;"></i>
        {label}<br>
        """

    legend_html += "</div>"

    mapa.get_root().html.add_child(folium.Element(legend_html))

    mapa.save(OUT_DIR / "mapa_clusters.html")

    print("Clustering completado")

# --------------------------------------------------
def main():
    print("===================================")
    print("INICIANDO PIPELINE DE ANÁLISIS")
    print("===================================")

    # --------------------------------------------------
    # 1. CARGA Y PREPARACIÓN
    # --------------------------------------------------
    print("\n[1] Cargando datos...")
    df = load_data(DATA_DIR)

    print("[2] Añadiendo variables temporales...")
    df = add_time_variables(df)

    # --------------------------------------------------
    # 2. DEMANDA BASE
    # --------------------------------------------------
    print("\n[3] Construyendo demanda diaria...")
    demanda_diaria = build_daily_demand(df)

    print("[4] Calculando patrón horario medio...")
    demanda = build_hourly_pattern(demanda_diaria)

    print("[5] Clasificando demanda por zona...")
    demanda = classify_demand(demanda)

    # --------------------------------------------------
    # 3. VISUALIZACIONES BÁSICAS
    # --------------------------------------------------
    print("\n[6] Generando heatmap principal...")
    plot_main_heatmap(demanda, OUT_DIR)

    print("[7] Generando curvas de zonas top...")
    plot_top_zones(demanda, OUT_DIR)

    print("[8] Generando boxplot...")
    plot_boxplot(demanda, OUT_DIR)

    # --------------------------------------------------
    # 4. ANÁLISIS SEMANAL
    # --------------------------------------------------
    print("\n[9] Analizando demanda por día de la semana...")
    demanda_semana = build_weekly_demand(df)

    print("[10] Generando curvas semanales...")
    plot_weekly_curves(demanda_semana, OUT_DIR)

    print("[11] Generando heatmap semanal...")
    plot_weekly_heatmap(demanda_semana, OUT_DIR)

    # --------------------------------------------------
    # 5. MAPAS
    # --------------------------------------------------
    print("\n[12] Generando mapa global...")
    map_global(demanda, DATA_DIR, OUT_DIR)

    print("[13] Generando mapa por zona...")
    map_local(demanda, DATA_DIR, OUT_DIR)

    # --------------------------------------------------
    # 6. CLUSTERING
    # --------------------------------------------------
    print("\n[14] Ejecutando clustering...")
    clustering_analysis(demanda, DATA_DIR, OUT_DIR)

    print("\n===================================")
    print("PIPELINE COMPLETADO")
    print("Resultados en:", OUT_DIR)
    print("===================================")


if __name__ == "__main__":
    main()