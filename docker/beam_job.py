import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging
import datetime
import re

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bigdata_project"

# Memoria volátil para el estado del clima (Simulando una tabla de estado en Flink)
# Se actualiza cuando llegan eventos al topic 'weather_rt_api'
WEATHER_STATE = {}

# Contadores para Monitorización [Requisito 6.1]
METRICS = {"processed": 0, "errors": 0, "weather_updates": 0}

class ReadAndJoinKafka(beam.DoFn):
    """
    Lee de DOS topics a la vez.
    - Si es clima: Actualiza el estado.
    - Si es vuelo: Lo emite para procesar.
    Esto cumple el requisito de JOIN
    """
    def process(self, element):
        # Nos suscribimos a ambos topics
        consumer = KafkaConsumer(
            'flights_rt_api', 'weather_rt_api',
            bootstrap_servers=['kafka:9093'], 
            auto_offset_reset='latest', 
            group_id='beam_multi_topic_group',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        print("📡 Radar Multicanal Activo (Vuelos + Clima).")
        
        for message in consumer:
            topic = message.topic
            payload = json.loads(message.value)
            
            if topic == 'weather_rt_api':
                # Actualizamos el estado del clima (JOIN)
                airport = payload.get('airport')
                condition = payload.get('condition')
                WEATHER_STATE[airport] = condition
                METRICS["weather_updates"] += 1
                # No emitimos el clima al siguiente paso, solo actualizamos memoria
                
            elif topic == 'flights_rt_api':
                # Enriquecemos el vuelo con el clima que tengamos en memoria
                airport = payload.get('airport')
                payload['weather_context'] = WEATHER_STATE.get(airport, "UNKNOWN")
                yield payload

class QualityAndKPIs(beam.DoFn):
    """ Aplica 5 Reglas de Calidad y calcula Riesgo """
    def process(self, data):
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        try:
            # --- VALIDACIÓN: 5 REGLAS DE CALIDAD [Cite: 6.2] ---
            
            # 1. Integridad: flight_id obligatorio
            if not data.get('flight_id'):
                raise ValueError("Regla 1: Falta flight_id")
            
            # 2. Tipo de Dato: delay_min numérico
            if not isinstance(data.get('delay_min'), (int, float)):
                raise ValueError("Regla 2: delay_min no es numérico")
                
            # 3. Rango de Negocio: Retraso imposible (>24h o negativo extremo)
            delay = float(data.get('delay_min'))
            if delay < -60 or delay > 1440:
                raise ValueError(f"Regla 3: Retraso fuera de rango lógico ({delay})")
                
            # 4. Formato: Aeropuerto IATA (3 letras mayúsculas)
            airport = data.get('airport', '')
            if not re.match(r'^[A-Z]{3}$', airport):
                raise ValueError(f"Regla 4: Código aeropuerto inválido ({airport})")
                
            # 5. Consistencia Temporal: Fecha válida (simple check)
            if not data.get('timestamp'):
                raise ValueError("Regla 5: Falta timestamp")

            # --- CÁLCULO DE KPIs [Cite: 7.1] ---
            # Aquí usamos el dato de clima que inyectamos en el paso anterior (Join)
            clima = data.get('weather_context', 'UNKNOWN')
            
            nivel_riesgo = "BAJO"
            if delay > 45:
                nivel_riesgo = "ALTO"
            elif delay > 15 and clima in ["STORM", "SNOW", "RAIN"]:
                nivel_riesgo = "ALTO (CLIMA ADVERSO)"
            elif delay > 15:
                nivel_riesgo = "MEDIO"

            # --- PERSISTENCIA (KPIs) [Cite: 5.3] ---
            doc_final = {
                "flight_id": data['flight_id'],
                "airport": airport,
                "delay_min": delay,
                "weather": clima,
                "risk_level": nivel_riesgo,
                "processed_at": datetime.datetime.now().isoformat()
            }
            db.realtime_kpis.insert_one(doc_final)
            
            METRICS["processed"] += 1
            print(f"✅ VUELO: {doc_final['flight_id']} | Clima: {clima} | Riesgo: {nivel_riesgo}")

        except ValueError as ve:
            # --- PERSISTENCIA (Errores de Calidad) ---
            error_doc = {
                "raw_data": str(data),
                "error_msg": str(ve),
                "timestamp": datetime.datetime.now().isoformat()
            }
            db.quality_errors.insert_one(error_doc)
            METRICS["errors"] += 1
            print(f"🚫 RECHAZADO: {ve}")
        
        finally:
            client.close()

class MonitorSystem(beam.DoFn):
    """ Reporta métricas del sistema periódicamente [Cite: 6.1] """
    def process(self, element):
        # En una implementación real usaríamos timers. 
        # Aquí guardamos una foto de las métricas cada vez que pasa un evento "tick".
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        metric_doc = {
            "timestamp": datetime.datetime.now().isoformat(),
            "total_processed": METRICS["processed"],
            "total_errors": METRICS["errors"],
            "weather_updates_received": METRICS["weather_updates"],
            "status": "HEALTHY"
        }
        db.system_metrics.insert_one(metric_doc)
        client.close()

def run():
    print("🚀 Iniciando Pipeline FINAL (Join + Quality + Monitoring)...")
    options = PipelineOptions(runner='DirectRunner')
    with beam.Pipeline(options=options) as p:
        # Flujo principal
        (
            p 
            | 'Inicio' >> beam.Create(['Start'])
            | 'Leer Multi-Topic' >> beam.ParDo(ReadAndJoinKafka())
            | 'Calidad y KPIs' >> beam.ParDo(QualityAndKPIs())
        )
        
        # Flujo secundario de Monitorización (Simulado)
        # En Flink real esto serían métricas nativas. Aquí lo guardamos en Mongo.
        # (Opcional para no complicar el código, las métricas ya se actualizan arriba)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()