# Reto Big Data: Meteorología y Retrasos de Vuelos  
### Arquitectura Lambda – Primera Entrega (11/12)

Este proyecto forma parte del **Reto Big Data**, cuyo objetivo es analizar la relación entre las condiciones meteorológicas y los retrasos en vuelos comerciales en aeropuertos de Estados Unidos.  

El equipo ha implementado la **primera fase** de una arquitectura **Lambda**, integrando ingesta, almacenamiento distribuido, procesamiento batch, procesamiento streaming y preparación para visualización en Power BI.

Esta entrega corresponde al **Hito 1 (11/12)**:
> *Diseño de la arquitectura, gobernanza inicial y configuración del entorno de trabajo.*

---

# 🧱 1. Arquitectura Lambda

El equipo ha diseñado la arquitectura completa siguiendo los componentes exigidos en el reto:  
**Apache NiFi, Apache Kafka, Apache Flink, Apache Spark, HDFS, SQL y Power BI.**

📌 El diagrama se encuentra en:  
➡ **`docs/architecture_diagram.png`**

### Resumen de la arquitectura:

- **Ingesta (NiFi):** lectura de CSVs históricos y consultas periódicas a las APIs OpenSky y OpenWeather.  
- **Almacenamiento RAW (HDFS):** zona donde se guardan los datos brutos provenientes de NiFi.  
- **Kafka:** recepción de los flujos de datos en tiempo real generados por NiFi.  
- **Batch Layer (Spark/PySpark):** lectura de históricos, validación, limpieza, transformación y preparación de datos para el Data Warehouse.  
- **Speed Layer (Flink):** lectura de mensajes en streaming, combinación vuelo+meteorología y cálculo inicial de KPIs.  
- **SQL Data Warehouse:** estructura dimensional para análisis histórico (dimensiones y hechos).  
- **NoSQL:** almacenamiento previsto para KPIs en tiempo real.  
- **Power BI:** herramienta donde se integrarán métricas históricas y de tiempo real.

---

# 🗂️ 2. Gobernanza del Dato

La gobernanza inicial del proyecto se encuentra documentada en:  
➡ **`docs/data_governance.md`**

Incluye:

### ✔ Topics de Kafka definidos
- `flights_rt_api`  
- `weather_rt_api`  
- `kpi_rt_by_airport` (planificado)

### ✔ Estructura de HDFS

/raw
   /flights        # Datos brutos de vuelos
   /weather        # Datos brutos de meteorología

/curated
   /flights        # Datos limpios y enriquecidos de vuelos
   /weather        # Datos limpios y enriquecidos de meteorología

/analytics
   /ml_datasets    # Conjuntos para machine learning

### ✔ Convenciones SQL
- `dim_airport`  
- `dim_airline`  
- `dim_date`  
- `fact_flight_delay`

---

# ⚙️ 3. Trabajo realizado por el equipo (11/12)

El equipo ha completado los puntos necesarios para la primera entrega, avanzando de forma coordinada en las distintas capas de la arquitectura Lambda:

---

## 🔸 Ingesta — Apache NiFi
- Configuración del entorno NiFi.  
- Creación del Processor Group para ingesta de CSVs históricos.  
- Creación del Processor Group para consultas periódicas a OpenSky y OpenWeather.  
- Validación de respuestas y gestión de rutas de error.  
- Estructura de ingesta funcional documentada con capturas (según informe interno).

---

## 🔸 Infraestructura y Streaming — Kafka & Flink
- Despliegue inicial mediante Docker Compose con:
  - Kafka  
  - Zookeeper  
  - Flink JobManager  
  - Flink TaskManager  
- Creación de los topics Kafka definidos en la gobernanza.  
- Desarrollo del primer job de Flink (“Hello World”): conexión al broker y lectura de mensajes desde Kafka.  
- Documentación técnica del proceso de arranque y prueba.

---

## 🔸 Batch Layer — PySpark & Procesamiento Histórico
- Preparación del entorno PySpark.  
- Lectura de datos históricos (`printSchema()` y `show(5)`).  
- Identificación de las columnas críticas para el modelo de retrasos.  
- Estructura base del Data Warehouse en SQL:
  - `dim_airport`, `dim_airline`, `dim_date`, `fact_flight_delay`.

---

## 🔸 Arquitectura, Gobernanza y Organización del Proyecto
- Diseño del diagrama completo de Arquitectura Lambda.  
- Documentación de gobernanza de datos (Kafka, HDFS, SQL).  
- Creación del repositorio GitHub.  
- Configuración de GitHub Projects para la planificación del trabajo.  
- Organización de la estructura base del proyecto en carpetas.

---

# 📁 4. Estructura del Repositorio

```text
/docs
   architecture_diagram.png
   architecture_diagram.drawio
   data_governance.md

/batch
   read_history.py

/streaming
   flink_hello_world.py

/nifi
   (flujos NiFi exportados)

/sql
   create_dw_tables.sql

/infra
   docker-compose.yml

README.md

