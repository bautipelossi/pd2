import os
import pandas as pd
import numpy as np
import joblib

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ==========================
# CONFIG
# ==========================

DATA_PATH = "datos/limpios/dataset_model.parquet"
MODEL_PATH = "src/05_despliegue/models/rf_model.pkl"
TARGET_COL = "income_rate"

# ==========================
# LOAD DATA
# ==========================

print("📦 Cargando dataset...")
df = pd.read_parquet(DATA_PATH)

print("Shape inicial:", df.shape)
print(df.head())

# ==========================
# LIMPIEZA BÁSICA
# ==========================

print("🧹 Limpiando datos...")

df = df.dropna()

# evitar divisiones raras o outliers extremos
df = df[df["income_rate"] > 0]
df = df[df["income_rate"] < 50]  # límite razonable

print("Shape después limpieza:", df.shape)

if "traffic_norm" not in df.columns:
    print("⚠️ traffic_norm no existe en el dataset. Se crea con 0.5 por defecto.")
    df["traffic_norm"] = 0.5

if "demand_score" not in df.columns:
    print("⚠️ demand_score no existe en el dataset. Se crea con 0.0 por defecto.")
    df["demand_score"] = 0.0

if "zone" not in df.columns:
    raise ValueError("Falta la columna zone para construir features por zona.")

# ==========================
# FEATURES
# ==========================

print("⚙️ Construyendo features...")

# ONE HOT ENCODING DE ZONA 🔥
df = pd.get_dummies(df, columns=["zone"])

# features finales
feature_cols = [col for col in df.columns if col != TARGET_COL]

X = df[feature_cols]
y = df[TARGET_COL]

print("Número de features:", X.shape[1])

traffic_unique = sorted(df["traffic_norm"].dropna().unique().tolist())
print("Valores únicos traffic_norm (muestra):", traffic_unique[:10])
if len(traffic_unique) == 1 and traffic_unique[0] == 0.5:
    print("⚠️ traffic_norm quedó constante en 0.5. Revisa llaves del merge de tráfico.")

demand_unique = sorted(df["demand_score"].dropna().unique().tolist())
print("Valores únicos demand_score (muestra):", demand_unique[:10])
if len(demand_unique) == 1 and demand_unique[0] == 0.0:
	print("⚠️ demand_score quedó constante en 0.0. Revisa la fuente/merge de demanda.")

# ==========================
# TRAIN / EVAL / SAVE
# ==========================

print("🚂 Entrenando RandomForest...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestRegressor(
    n_estimators=300,
    random_state=42,
    n_jobs=-1,
    min_samples_leaf=2,
)
model.fit(X_train, y_train)

pred = model.predict(X_test)

mae = mean_absolute_error(y_test, pred)
rmse = np.sqrt(mean_squared_error(y_test, pred))
r2 = r2_score(y_test, pred)

print("📈 Métricas")
print(f"MAE : {mae:.4f}")
print(f"RMSE: {rmse:.4f}")
print(f"R²  : {r2:.4f}")

os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
joblib.dump(model, MODEL_PATH)
print(f"💾 Modelo guardado en: {MODEL_PATH}")