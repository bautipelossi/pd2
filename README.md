# 🚕 NYC Traffic & Mobility Analytics by Taxómanos

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> *Extracción, transformación, análisis y visualización de datos de taxis amarillos, los servicios VTC (Uber/Lyft), el tráfico vehicular, eventos y clima en la ciudad de Nueva York durante 2023.*)

---

##  Índice
1. [Descripción del Proyecto](#descripcion)
2. [Orígenes de Datos](#origenes)
3. [Estructura del Proyecto](#estructura)
4. [Instalación y Requisitos](#instalacion)
5. [Uso y Ejecución](#uso)
6. [Próximos Pasos (Future Work)](#futuro)
7. [Autores (Grupo Taxómanos)](#autores)

---

<a id="descripcion"></a>
##  Descripción del Proyecto

**Objetivos principales:**
* Objetivo 1: Explorar y extraer datos útiles sobre taxis, ubers, clima, eventos y tráfico en NYC.
* Objetivo 2: Limpiar, transformar y unificar datasets masivos usando formato Parquet.
* Objetivo 3: Analizar patrones temporales de movilidad.
* Objetivo 4: Desarrollar gráficos interactivos en HTML para la extracción de conclusiones (zonas con más afluencia, horas con más afluencia, motivos de ello...).

---

<a id="origenes"></a>
## Orígenes de Datos

Los datos utilizados en este proyecto provienen de fuentes públicas y han sido procesados para su análisis:

* **Yellow Taxi Trip Data (YLC)**: Este Dataset proviene del catálogo de la Comisión de Taxis y Limusinas (TLC) del Ayuntamiento de Nueva York (publicado en NYC OpenData) y contiene registros de los viajes realizados en Taxis Amarillos a lo largo del año 2023 en la ciudad de Nueva York. Cada fila del dataset corresponde a un solo viaje en taxi, e incluye fechas y horas de inicio y final de viaje, ID de las zonas de inicio y final según el sistema de Taxi Zones, número de pasajeros, distancia recorrida, coste del viaje, tarifas adicionales, propinas y forma en la que se realizó el pago.

* **Vehículos de Alquiler (Uber/Lyft - FHV):** (High-Volume For-Hire Vehicle (FHV) Trip Data 2023) Este dataset de la mencionada TLC contiene registros detallados de viajes realizados por vehículos de alquiler de alto volumen durante el año 2023. Se consideran FHV de alto volumen aquellas compañías que superan los 10.000 viajes mensuales (como Uber o Lyft). Cada registro corresponde a un viaje individual y contiene variables temporales, espaciales y económicas.. En particular, incluye la fecha y hora de recogida y finalización, los identificadores de la zona de origen y destino según el sistema oficial de “Taxi Zones” de NYC, la distancia recorrida en millas y la duración del trayecto, así como distintos componentes tarifarios (tarifa base abonada por el pasajero, los peajes, las propinas y pago al conductor).

* **Tráfico de NYC:** El dataset es Traffic Volume Counts (Historical) y viene de la web NYC OpenData. Este es un conjunto de datos histórico estático. El Departamento de Transporte de la Ciudad de Nueva York (NYC DOT) utiliza Registradores Automáticos de Tráfico (ATR) para recopilar recuentos de muestras del volumen de tráfico en cruces de puentes y carreteras.

* **Eventos en NYC:** El dataset es NYC Permitted Event Information - Historical y viene de la web NYC OpenData. Se trata de un conjunto de datos histórico estático que recoge información sobre eventos que requieren permiso oficial en la ciudad de Nueva York, como desfiles, festivales, carreras, eventos culturales, rodajes o concentraciones públicas. Cada registro incluye información como el nombre del evento, tipo de evento, agencia responsable, distrito (borough), localización, y fechas y horas de inicio y finalización. Este dataset permite identificar eventos multitudinarios que potencialmente pueden alterar los patrones normales de tráfico y movilidad en la ciudad.

* **Partidos de la MLB en NYC:** Utilizando la MLB Stats API, la API oficial de datos de Major League Baseball (MLB), hemos obtenido datos sobre partidos de béisbol en los dos estadios de los equipos más importantes de Nueva York, Yankee Stadium y Citi Field. Hemos utilizado estos datos para complementar los obtenidos de eventos, ya que estos partidos son los que más gente mueven y pueden alterar el tráfico significativamente.

* **Datos meteorológicos en NYC:** Para incorporar condiciones meteorológicas, se utilizó la API de Open-Meteo, un servicio gratuito que proporciona datos históricos. Se descargaron variables de temperatura, precipitaciones y niveles de nieve en cada hora para la ciudad de Nueva York. Estos datos permiten controlar el efecto del clima sobre el volumen de tráfico.

* **Restaurantes en NYC**: Dataset de restaurantes de Nueva York basado en información de Google Maps, obtenido desde Kaggle. Incluye nombre, rating, cantidad de reseñas, categoría de precio, dirección, coordenadas y código postal. Se utiliza como capa socioeconómica complementaria para analizar distribución de oferta gastronómica y su relación con zonas de demanda.

* **Precios de Alquiler en NYC**: Dataset ligero de propiedades en alquiler de Nueva York, obtenido desde Kaggle. Incluye barrio, latitud, longitud, tipo de habitación, precio y variables de ocupación/reseñas. Este dataset se agrega espacialmente por Taxi Zone para construir indicadores de alquiler medio y mediano como proxy socioeconómico adicional.

---

<a id="estructura"></a>
## Estructura del Proyecto

El repositorio está organizado de forma modular, separando claramente los datos crudos, los datos procesados y el código fuente:

```text
📁 pd2/
│
├── 📁 /               # Directorio principal de la entrega
│   │
|   |
│   ├── 📁 src/                    # Código fuente del proyecto
│   │   │
│   │   ├── 📁 Extraccion/         # Scripts de Extracción
│   │   │   ├── ClimateNYC.py        
│   │   │   ├── FHV.py
│   │   │   ├── LTC.py
│   │   │   ├── NYCevents.py  
│   │   │   └── SportEventsNYC.py
│   │   │
│   │   ├── 📁 Transformacion/     # Scripts de Transformación y Limpieza
│   │   │   ├── Cleaning_FHV.py      
│   │   │   ├── Cleaning_LTC.py
│   │   │   ├── Cleaning_NYCevents.py        
│   │       ├── agregaciones.py
│   │       ├── agregaciones_hora.py      
│   │   │   └── PreprocesamientoVolumenTrafico.py 
│   │   │
│   │   └── 📁 Visualizacion/      # Scripts para el análisis cruzado y generación de gráficos
│   │       ├── YLC_vs_FHV_clima.py
│   │       ├── VisualizacionTrafico.py
│   │       ├── agregaciones_hora.py
│   │       ├── prueba_barrios.py
│   │       ├── visualizacion_agregaciones_con_trafico.py
│   │       ├── visualizacionfhv.py
|   |       ├── Visualizacion_Events.py
│   │       │
│   │       ├── 📁 Mapa_Interactivo_FHV_TLC/ # Outputs: Gráficos HTML interactivos relacionando solo FHV y TLC
│   │       │
│   │       ├── 📁 Reporte_Trafico_NYC/      # Outputs: Gráficos HTML interactivos relacionados con el tráfico
|   |       |
|   |       └── 📁 YLC_FHV_clima/            # Outputs: Gráfico HTML interactivo analizando el crecimiento de demanda de FHV y YLC 
│
├── requirements.txt               # Archivo para instalar las librerías del proyecto
│
└── README.md                      # Este archivo de documentación
```

---

<a id="instalacion"></a>
##  Instalación y Requisitos

Para replicar este proyecto en tu máquina local, sigue estos pasos:

1. **Clona el repositorio:**
   ```bash
   git clone https://github.com/bautipelossi/pd2.git
   cd pd2
   ```

2. **Crea un entorno virtual**
   ```bash
   python -m venv env
   source env/bin/activate  # En Windows: env\Scripts\activate
   ```

3. **Instala las dependencias**
   ```bash
   pip install -r requirements.txt
   ```

Para el uso de MinIO también es necesaria la creación de un .env con los siguientes parámetros
   ```bash
   MINIO_ENDPOINT=minio.fdi.ucm.es
   MINIO_ACCESS_KEY=TU_ACESS_KEY_PERSONAL
   MINIO_SECRET_KEY=TU_SECRET_KEY_PERSONAL
   MINIO_BUCKET=pd2
   MINIO_GROUP_PATH=taxomanos
   ```

---

<a id="uso"></a>
##  Uso y Ejecución

Para replicar el análisis de este proyecto correctamente, debes seguir el flujo lógico de los datos (Pipeline ETL). El orden de ejecución de las carpetas es el siguiente: **1. Extracción ➔ 2. Transformación ➔ 3. Visualización** (ojo con las rutas de los datos).

### Paso 1: Extracción de Datos
Primero, ejecuta los scripts de la carpeta `Extraccion` para obtener los datos crudos (clima, eventos, viajes de FHV/LTC, etc.):

```bash
python src/Extraccion/FHV.py
python src/Extraccion/LTC.py
# (Ejecutar el resto de scripts según los datos que necesites actualizar)
```

### Paso 2: Transformación y Limpieza

Una vez tengas los datos originales, ejecuta los scripts de la carpeta `Transformacion`. Estos scripts limpiarán los datos, unificarán formatos y generarán los archivos `.parquet` optimizados y listos para el análisis:

```bash
python /src/Transformacion/Cleaning_FHV.py
python /src/Transformacion/Cleaning_LTC.py
python /src/Transformacion/PreprocesamientoVolumenTrafico.py
# (Continúa con los demás scripts de limpieza correspondientes)
```

### Paso 3: Visualización

Con los datos procesados, finalmente puedes ejecutar los scripts de la carpeta `Visualizacion` para generar los gráficos interactivos. Tienes varios scripts dependiendo del análisis que quieras realizar:

* Para el análisis general de demanda cruzada con el clima:
  ```bash
  python /src/Visualizacion/LTC_vs_FHV_clima.py
  ```
  (Los resultados de visualización general se exportarán a la subcarpeta Mapa_Interactivo_FHV_TLC/)

* Para la comparativa cruzada de demanda vs. tráfico vehicular:
  ```bash
  python /src/Visualizacion/visualizacion_agregaciones_con_trafico.py
  ```
  (Los resultados y gráficos .html interactivos se guardarán automáticamente en la subcarpeta Reporte_Trafico_NYC/)

---

<a id="futuro"></a>
##  Próximos Pasos (Future Work)

Para ampliar el alcance y la profundidad de este análisis, se proponen las siguientes mejoras futuras:

* Entrenar un modelo predictivo (Machine Learning) basado en series temporales para estimar zonas de alta demanda de transporte en función del día de la semana y eventos programados.
* Desplegar un dashboard interactivo utilizando Streamlit o Dash para permitir la exploración dinámica de los datos por parte de usuarios no técnicos.

---

<a id="autores"></a>
## Autores (Grupo Taxómanos)

* **Pablo Alonso** - *Data Scientist / Developer*
* **Rodrigo Jesús-Portanet** - *Data Scientist / Developer*
* **Bautista Pelossi Schweizer** - *Data Scientist / Developer*
* **Óscar Marín** - *Data Scientist / Developer*
