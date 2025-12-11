# Gobernanza de Datos – Arquitectura Lambda

## 1. Topics de Kafka

- flights_rt_api      # vuelos en tiempo real desde OpenSky (NiFi → Kafka)
- weather_rt_api      # meteorología en tiempo real desde OpenWeather (NiFi → Kafka)
- kpi_rt_by_airport   # (opcional) KPIs de retraso/riesgo por aeropuerto

Reglas:
- snake_case
- sin espacios ni mayúsculas
- nombre funcional y entendible

---

## 2. Estructura de carpetas en HDFS

/raw
  /flights      # históricos de retrasos (delays_history_agg, sample)
/raw
  /weather      # histórico meteorológico (weather_history)

/curated
  /flights      # datos limpios/enriquecidos de vuelos
/curated
  /weather      # datos limpios/enriquecidos de meteo

/analytics
  /ml_datasets  # datasets de entrenamiento/test para ML

Convención:
- raw = datos tal cual llegan (NiFi)
- curated = datos limpios/agrupados (Spark)
- analytics = datasets específicos de análisis/ML

---

## 3. Convención de nombres en BBDD SQL

Base de datos: air_delay_dw

Tablas de dimensiones:
- dim_airport
- dim_airline
- dim_date

Tabla de hechos:
- fact_flight_delay

Reglas:
- prefijo dim_ para dimensiones
- prefijo fact_ para tablas de hechos
- snake_case en nombres de columnas