import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer # Usamos la librería pura de Python
import logging
import sys

# Definimos una "Transformación" personalizada para leer de Kafka
# Esto evita el problema de necesitar Java instalado en el contenedor
class ReadFromKafka(beam.DoFn):
    def process(self, element):
        # Configuración para conectar con el contenedor de Kafka
        # IMPORTANTE: Usamos el puerto 9093 que es el interno de Docker
        consumer = KafkaConsumer(
            'flights_rt_api',
            bootstrap_servers=['kafka:9093'], 
            auto_offset_reset='earliest',
            group_id='beam_worker_group',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        
        print("📡 Radar Beam encendido. Escuchando en kafka:9093...")
        
        # Bucle infinito leyendo mensajes
        for message in consumer:
            yield message.value

def run():
    print("🚀 Iniciando Pipeline de Apache Beam (Pure Python)...")
    
    # Usamos DirectRunner (Ejecución local)
    options = PipelineOptions(runner='DirectRunner')

    with beam.Pipeline(options=options) as p:
        (
            p 
            | 'Inicio' >> beam.Create(['Start']) # Un disparador inicial
            | 'Conectar Kafka' >> beam.ParDo(ReadFromKafka())
            | 'Imprimir' >> beam.Map(lambda msg: print(f"✅ DATO RECIBIDO: {msg}"))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()