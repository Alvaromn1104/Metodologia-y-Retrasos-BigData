import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging
import datetime

# --- CONFIGURACIÓN ---
# Datos para conectar a Mongo (definidos en docker-compose)
MONGO_URI = "mongodb://root:example@mongo:27017/"
DB_NAME = "bigdata_project"
COLLECTION_NAME = "realtime_kpis"

class ReadFromKafka(beam.DoFn):
    """Lee mensajes de Kafka y los decodifica"""
    def process(self, element):
        # Conectamos a Kafka en el puerto interno de Docker (9093)
        consumer = KafkaConsumer(
            'flights_rt_api',
            bootstrap_servers=['kafka:9093'], 
            auto_offset_reset='latest', # Leer solo los nuevos mensajes
            group_id='beam_writer_group',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        print("📡 Radar Beam conectado a Kafka. Esperando vuelos...")
        for message in consumer:
            yield message.value

class ProcessAndWriteToMongo(beam.DoFn):
    """Calcula KPIs sencillos y guarda en MongoDB"""
    def process(self, element):
        try:
            # 1. Convertir texto JSON a Diccionario Python
            data = json.loads(element)
            
            # 2. CÁLCULO DE KPI (Simulación de Lógica de Negocio)
            # Si el retraso es mayor a 15 min, es RIESGO ALTO
            retraso = data.get('delay_min', 0)
            if retraso > 15:
                data['nivel_riesgo'] = 'ALTO'
            else:
                data['nivel_riesgo'] = 'BAJO'
            
            # Añadimos fecha de procesamiento
            data['processed_at'] = datetime.datetime.now().isoformat()

            # 3. GUARDAR EN MONGODB
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            # Insertamos el documento
            rec_id = collection.insert_one(data).inserted_id
            
            print(f"GUARDADO EN MONGO (ID: {rec_id}) -> Vuelo: {data.get('flight_id')} | Riesgo: {data['nivel_riesgo']}")
            client.close()
            
        except Exception as e:
            logging.error(f"Error procesando/guardando: {e}")

def run():
    print("Iniciando Pipeline Beam (Kafka -> Mongo)...")
    options = PipelineOptions(runner='DirectRunner')
    with beam.Pipeline(options=options) as p:
        (
            p 
            | 'Inicio' >> beam.Create(['Start'])
            | 'Leer Kafka' >> beam.ParDo(ReadFromKafka())
            | 'Procesar y Guardar' >> beam.ParDo(ProcessAndWriteToMongo())
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()