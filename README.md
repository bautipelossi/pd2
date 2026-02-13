# 🚕 [Nombre de tu Proyecto] (Ej: NYC Traffic & Mobility Analytics)

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> *Extracción, transformación, análisis y visualización de datos de taxis amarillos, los servicios VTC (Uber/Lyft), el tráfico vehicular, eventos y clima en la ciudad de Nueva York durante 2023.*)

---

## 📑 Índice
1. [Descripción del Proyecto](#descripcion)
2. [Orígenes de Datos](#origenes)
3. [Estructura del Proyecto](#estructura)
4. [Instalación y Requisitos](#instalacion)
5. [Uso y Ejecución](#uso)
6. [Próximos Pasos (Future Work)](#futuro)
7. [Autores (Grupo Taxómanos)](#autores)

---

<a id="descripcion"></a>
## 💡 Descripción del Proyecto

**Objetivos principales:**
* Objetivo 1: Explorar y extraer datos útiles sobre taxis, ubers, clima, eventos y tráfico en NYC.
* Objetivo 2: Limpiar, transformar y unificar datasets masivos usando formato Parquet.
* Objetivo 3: Analizar patrones temporales de movilidad.
* Objetivo 4: Desarrollar gráficos interactivos en HTML para la extracción de conclusiones (zonas con más afluencia, horas con más afluencia, motivos de ello...).

---

<a id="origenes"></a>
## 📊 Orígenes de Datos

Los datos utilizados en este proyecto provienen de fuentes públicas y han sido procesados para su análisis:

* **Taxis Amarillos (YLC):** [Explicar brevemente de dónde viene, ej: NYC TLC Trip Record Data].
* **Vehículos de Alquiler (Uber/Lyft - FHV):** [Explicar brevemente].
* **Tráfico de NYC:** [Explicar qué mide este dataset].
* **Taxi Zone Lookup:** Archivo oficial para mapear los Location IDs con los distritos (Boroughs) de Nueva York.

---

<a id="estructura"></a>
## 📁 Estructura del Proyecto

El repositorio está organizado de forma modular, separando claramente los datos crudos, los datos procesados y el código fuente:

```text
📁 pd2/
│
├── 📁 Entrega1_Pd2/               # Directorio principal de la entrega
│   │
│   ├── 📁 src/                    # Código fuente del proyecto
│   │   │
│   │   ├── 📁 Extraccion/       # Scripts de Extracción
│   │   │   ├── ClimateNYC.py       
│   │   │   ├── FHV.py
│   │   │   ├── LTC.py
│   │   │   ├── NYCevents.py  
│   │   │   └── SportEventsNYC.py
│   │   ├── 📁 Transformacion/     # Scripts de Extracción
│   │   │   ├── Cleaning_FHV.py      
│   │   │   ├── Cleaning_LTC.py
│   │   │   ├── Cleaning_NYCevents.py       
│   │   │   ├── Cleaning_SportEventsNYC.py      
│   │   │   ├── PreprocesamientoVolumenTrafico.py         
│   │   │   └── VolumenTraficoParquet.py
│   │   │
│   │   └── 📁 Visualizacion/      # Scripts para el análisis cruzado y generación de gráficos
│   │       ├── LTC_vs_FHV_clima.py
│   │       ├── VisualizacionTrafico.py
│   │       ├── agregaciones.py
│   │       ├── agregaciones_hora.py
│   │       ├── prueba_barrios.py
│   │       ├── visualizacion_agregaciones_con_trafico.py
│   │       ├── visualizacionfhv.py
│   │       │
│   │       ├── 📁 Mapa_Interactivo_FHV_TLC/ # Outputs: Gráficos HTML interactivos generados por el código relacionando solo FHV y TLC
│   │       │
│   │       └── 📁 Reporte_Trafico_NYC/ # Outputs: Gráficos HTML interactivos generados por el código relacionados con el tráfico
│
└── README.md                      # Este archivo de documentación
```
---

<a id="instalacion"></a>
## ⚙️ Instalación y Requisitos

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
   pip install pandas...
   ```

---

<a id="uso"></a>
## 🚀 Uso y Ejecución

Para replicar el análisis de este proyecto correctamente, debes seguir el flujo lógico de los datos (Pipeline ETL). El orden de ejecución de las carpetas es el siguiente: **1. Extracción ➔ 2. Transformación ➔ 3. Visualización** (ojo con las rutas de los datos).

### Paso 1: Extracción de Datos
Primero, ejecuta los scripts de la carpeta `Extraccion` para obtener los datos crudos (clima, eventos, viajes de FHV/LTC, etc.):

```bash
python Entrega1_Pd2/src/Extraccion/FHV.py
python Entrega1_Pd2/src/Extraccion/LTC.py
# (Ejecutar el resto de scripts según los datos que necesites actualizar)
```

### Paso 2: Transformación y Limpieza

Una vez tengas los datos originales, ejecuta los scripts de la carpeta `Transformacion`. Estos scripts limpiarán los datos, unificarán formatos y generarán los archivos `.parquet` optimizados y listos para el análisis:

```bash
python Entrega1_Pd2/src/Transformacion/Cleaning_FHV.py
python Entrega1_Pd2/src/Transformacion/Cleaning_LTC.py
python Entrega1_Pd2/src/Transformacion/PreprocesamientoVolumenTrafico.py
# (Continúa con los demás scripts de limpieza correspondientes)
```

### Paso 3: Visualización

Con los datos procesados, finalmente puedes ejecutar los scripts de la carpeta `Visualizacion` para generar los gráficos interactivos. Tienes varios scripts dependiendo del análisis que quieras realizar:

* Para el análisis general de demanda cruzada con el clima:
  ```bash
  python Entrega1_Pd2/src/Visualizacion/LTC_vs_FHV_clima.py
  ```
  (Los resultados de visualización general se exportarán a la subcarpeta Mapa_Interactivo_FHV_TLC/)
  
* Para la comparativa cruzada de demanda vs. tráfico vehicular:
  ```bash
  python Entrega1_Pd2/src/Visualizacion/visualizacion_agregaciones_con_trafico.py
  ```
  (Los resultados y gráficos .html interactivos se guardarán automáticamente en la subcarpeta Reporte_Trafico_NYC/)

---

<a id="futuro"></a>
## 🔮 Próximos Pasos (Future Work)

Para ampliar el alcance y la profundidad de este análisis, se proponen las siguientes mejoras futuras:

* Entrenar un modelo predictivo (Machine Learning) basado en series temporales para estimar zonas de alta demanda de transporte en función del día de la semana y eventos programados.
* Desplegar un dashboard interactivo utilizando Streamlit o Dash para permitir la exploración dinámica de los datos por parte de usuarios no técnicos.

---

<a id="autores"></a>
## ✒️ Autores (Grupo Taxómanos)

* **Pablo Alonso** - *Data Analyst / Developer*
* **Rodrigo Jesús-Portanet** - *Data Analyst / Developer*
* **Bautista Pelossi** - *Data Analyst / Developer*
* **Óscar Marín** - *Data Analyst / Developer*





