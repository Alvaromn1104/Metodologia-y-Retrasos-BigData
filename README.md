# Ecosistema Big Data para el Análisis Predictivo de Retrasos de Vuelos ✈️🌦️

## 📌 Descripción del Proyecto

Este proyecto implementa un **ecosistema Big Data basado en una Arquitectura Lambda** para el análisis histórico y en tiempo real de los retrasos de vuelos, incorporando variables meteorológicas como factor clave de predicción.

El objetivo principal es **detectar y predecir retrasos superiores a 15 minutos**, combinando procesamiento batch, streaming y modelos de Machine Learning, y ofreciendo una **visualización estratégica mediante Power BI** para la toma de decisiones operativas.

---

## 🧠 Arquitectura General

El sistema sigue una **Arquitectura Lambda**, dividiendo el procesamiento de datos en dos grandes capas:

### 🔹 Batch Layer (Histórico)
- Procesamiento de grandes volúmenes de datos históricos (2019–2023).
- Limpieza, normalización y enriquecimiento de datos de vuelos y clima.
- Modelado dimensional (Esquema en Estrella) en MySQL.
- Entrenamiento de modelos de Machine Learning.

### 🔹 Speed Layer (Tiempo Real)
- Ingesta de eventos en tiempo real con Apache Kafka.
- Procesamiento y reglas de negocio con Apache Beam.
- Clasificación de riesgo operativo en tiempo real.
- Persistencia rápida en MongoDB para dashboards en vivo.

---

## 🛠️ Tecnologías Utilizadas

- **Apache Kafka** – Mensajería y streaming de eventos
- **Apache Spark / PySpark** – Procesamiento batch
- **Apache Beam** – Procesamiento en streaming
- **Docker & Docker Compose** – Contenerización del ecosistema
- **MySQL** – Data Warehouse (modelo dimensional)
- **MongoDB** – Almacenamiento NoSQL para datos en tiempo real
- **Power BI** – Visualización y análisis de datos
- **Databricks** – Entrenamiento de modelos y análisis avanzado
- **Python** – ETL y Machine Learning

---

## 📂 Organización del Proyecto

```text
├── docker/
│   └── docker-compose.yml
├── kafka/
│   ├── producers/
│   └── topics/
├── batch/
│   ├── etl_pyspark/
│   └── modelado_mysql/
├── speed/
│   └── beam_pipeline/
├── ml/
│   └── modelo_regresion_logistica/
├── dashboards/
│   └── power_bi/
└── README.md
```

## 🔄 Procesamiento Batch (ETL Histórico)

Durante el procesamiento histórico se realizaron las siguientes tareas:

- **Unificación** de múltiples fuentes de datos meteorológicos.
- **Normalización** de unidades y estandarización de aeropuertos bajo el estándar **ICAO**.
- **Joins complejos** entre datos de vuelos y datos meteorológicos.
- **Creación de un modelo en estrella**, compuesto por:
  - `Dim_Aeropuerto`
  - `Dim_Aerolinea`
  - `Dim_Tiempo`
  - `Fact_Retrasos_Historicos`

Este diseño optimiza el rendimiento de las consultas analíticas y facilita la explotación de datos en **Power BI**.

---

## 🤖 Machine Learning

Se entrenó un **modelo de Regresión Logística** cuyo objetivo es predecir si un vuelo sufrirá un retraso superior a **15 minutos**.

### Resultados destacados:
- **Accuracy aproximado:** 60%

### Variables más relevantes:
- Visibilidad
- Velocidad del viento
- Temperatura

El modelo sirve como una base funcional sólida para la detección de **riesgo operativo**, con margen de mejora futura.

---

## ⚡ Procesamiento en Tiempo Real (Speed Layer)

- Ingesta de eventos de vuelos y clima mediante **Apache Kafka**.
- Validación automática de mensajes inválidos.
- Enriquecimiento en memoria con datos meteorológicos.
- Clasificación de riesgo:
  - **BAJO**
  - **MEDIO**
  - **ALTO**
- Persistencia en **MongoDB** para acceso inmediato desde **Power BI**.

---

## 📊 Visualización en Power BI

El dashboard final ofrece tres ejes clave:

### 🔹 Visión Operativa
Mapa de calor que muestra los aeropuertos con mayor estrés en tiempo real.

### 🔹 Análisis Meteo–Retrasos
Relación directa entre las condiciones climáticas y los minutos de retraso.

### 🔹 Comparativa de Rendimiento
Ranking de aerolíneas según su eficiencia operativa bajo condiciones climáticas similares.

---

## 🚀 Despliegue del Proyecto

### Requisitos
- Docker
- Docker Compose
- Git

### Ejecución
```bash
docker-compose up -d
```
