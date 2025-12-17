import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging
import datetime
import re
import time

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bigdata_project"

# --- DICCIONARIO DE TRADUCCIÓN (Crucial para el Join) ---
# OpenWeather nos da nombres de ciudades ("Sol", "Atlanta"), pero OpenSky usa códigos ICAO ("LEMD", "KATL").
# Este diccionario permite cruzar ambos mundos.
CITY_TO_ICAO = {
    "Sol": "LEMD",      # Madrid
    "Madrid": "LEMD",
    "Atlanta": "KATL",  # Atlanta Hartsfield-Jackson
    "London": "EGLL",   # Heathrow
    "New York": "KJFK"  # JFK
}

# Estado del clima en memoria (Broadcast State)
WEATHER_STATE = {}
METRICS = {"processed": 0, "errors": 0, "weather_updates": 0}

class ReadAndJoinKafka(beam.DoFn):
    """
    Lee datos RAW de las APIs, los parsea y realiza el JOIN.
    """
    def process(self, element):
        consumer = None
        while consumer is None:
            try:
                consumer = KafkaConsumer(
                    'flights_rt_api', 'weather_rt_api',
                    bootstrap_servers=['kafka:9093'], 
                    auto_offset_reset='latest', 
                    group_id='beam_final_group',
                    value_deserializer=lambda x: x.decode('utf-8')
                )
            except Exception:
                time.sleep(5)

        print("📡 Escuchando Kafka (Formatos Reales OpenSky/OpenWeather)...")
        
        for message in consumer:
            topic = message.topic
            try:
                raw = json.loads(message.value)
                
                # --- LOGICA PARA API WEATHER (OpenWeather) ---
                if topic == 'weather_rt_api':
                    # Parseamos la estructura compleja de OpenWeather
                    city_name = raw.get('name', 'Unknown')
                    
                    # Extraemos condición (está dentro de una lista)
                    condition = "Unknown"
                    if 'weather' in raw and len(raw['weather']) > 0:
                        condition = raw['weather'][0]['main'] # Ej: "Clouds", "Rain"
                    
                    # Traducimos Ciudad -> Aeropuerto para poder hacer el Join luego
                    airport_code = CITY_TO_ICAO.get(city_name, "UNKNOWN")
                    
                    if airport_code != "UNKNOWN":
                        WEATHER_STATE[airport_code] = condition
                        METRICS["weather_updates"] += 1
                        print(f"🌤 CLIMA ACTUALIZADO: {airport_code} ({city_name}) -> {condition}")

                # --- LOGICA PARA API FLIGHTS (OpenSky) ---
                elif topic == 'flights_rt_api':
                    # Normalizamos el dato de vuelo a nuestro formato interno
                    # NOTA: Asumimos que NiFi ha calculado el 'delay_calculated'
                    
                    flight_data = {
                        "flight_id": raw.get('callsign', '').strip(), # Quitamos espacios "SWA352 "
                        "airport": raw.get('estDepartureAirport'),     # Ej: "KATL"
                        "delay_min": raw.get('delay_calculated', 0),   # Dato inyectado por NiFi
                        "raw_timestamp": raw.get('firstSeen'),
                        # Buscamos el clima en memoria (JOIN)
                        "weather_context": WEATHER_STATE.get(raw.get('estDepartureAirport'), "CLEAR")
                    }
                    yield flight_data

            except Exception as e:
                print(f"⚠️ Error parseando mensaje Kafka: {e}")

class QualityAndKPIs(beam.DoFn):
    def process(self, data):
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        try:
            # --- 5 REGLAS DE CALIDAD [Cite: 6.2] ---
            
            # Regla 1: Identificador de vuelo obligatorio
            if not data['flight_id']:
                raise ValueError("Callsign (flight_id) vacío")
                
            # Regla 2: Aeropuerto de origen obligatorio
            if not data['airport']:
                raise ValueError("Aeropuerto de origen nulo")
                
            # Regla 3: Retraso numérico
            if not isinstance(data['delay_min'], (int, float)):
                raise ValueError("El retraso no es un número válido")
                
            # Regla 4: Formato ICAO (4 letras para aeropuerto)
            if len(data['airport']) != 4:
                raise ValueError(f"Código aeropuerto inválido: {data['airport']}")
                
            # Regla 5: Coherencia temporal (timestamp existe)
            if not data['raw_timestamp']:
                raise ValueError("Falta timestamp (firstSeen)")

            # --- CÁLCULO DE KPIs [Cite: 7.1] ---
            delay = float(data['delay_min'])
            clima = data['weather_context']
            
            nivel_riesgo = "BAJO"
            if delay > 45:
                nivel_riesgo = "ALTO"
            elif delay > 15 and clima in ["Thunderstorm", "Rain", "Snow"]: # Valores de OpenWeather
                nivel_riesgo = "ALTO (CLIMA ADVERSO)"
            elif delay > 15:
                nivel_riesgo = "MEDIO"

            # Documento final para Power BI
            doc_final = {
                "flight_id": data['flight_id'],
                "airport": data['airport'],
                "delay_min": delay,
                "weather_condition": clima,
                "risk_level": nivel_riesgo,
                "processed_at": datetime.datetime.now().isoformat()
            }
            
            # Guardamos en REALTIME_KPIS
            db.realtime_kpis.insert_one(doc_final)
            METRICS["processed"] += 1
            print(f"✅ VUELO: {doc_final['flight_id']} | Clima: {clima} | Riesgo: {nivel_riesgo}")

        except ValueError as ve:
            # Guardamos en QUALITY_ERRORS [Cite: 6.2]
            error_doc = {"raw_data": str(data), "error_msg": str(ve), "at": datetime.datetime.now().isoformat()}
            db.quality_errors.insert_one(error_doc)
            METRICS["errors"] += 1
            print(f"🚫 CALIDAD RECHAZADA: {ve}")
        
        finally:
            client.close()

def run():
    options = PipelineOptions(runner='DirectRunner')
    with beam.Pipeline(options=options) as p:
        (p | 'Inicio' >> beam.Create(['Start'])
           | 'Leer y Unir' >> beam.ParDo(ReadAndJoinKafka())
           | 'KPIs y Calidad' >> beam.ParDo(QualityAndKPIs()))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()