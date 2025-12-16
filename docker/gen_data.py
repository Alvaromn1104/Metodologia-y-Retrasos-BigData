from kafka import KafkaProducer
import json
import time
import random

BROKERS = ['kafka:9093']
producer = KafkaProducer(bootstrap_servers=BROKERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

aeropuertos = ["MAD", "BCN", "JFK", "LHR"]
climas = ["CLEAR", "RAIN", "STORM", "SNOW", "CLOUDY"]

print("🌪️ Iniciando SIMULACIÓN TOTAL (Vuelos + Clima + Errores)...")

for i in range(50):
    
    # 1. Enviamos actualización de CLIMA (Topic weather_rt_api)
    # Esto permite probar el JOIN en tiempo real
    airport_update = random.choice(aeropuertos)
    weather_event = {
        "airport": airport_update,
        "condition": random.choice(climas),
        "timestamp": time.time()
    }
    producer.send('weather_rt_api', weather_event)
    
    # 2. Enviamos VUELO (Topic flights_rt_api)
    airport_flight = random.choice(aeropuertos)
    delay = random.randint(-10, 150) # Incluye negativos para testear rangos
    
    # Introducimos ERRORES A PROPÓSITO para probar Reglas de Calidad
    flight_id = f"IB-{random.randint(1000, 9999)}"
    if i % 10 == 0: 
        flight_id = "" # Error Regla 1: Falta ID
    if i % 15 == 0:
        airport_flight = "Madrid" # Error Regla 4: Formato incorrecto (no es IATA)

    flight_event = {
        "flight_id": flight_id,
        "airport": airport_flight,
        "timestamp": "2025-12-18T12:00:00",
        "delay_min": delay,
        "status": "UNK"
    }
    
    producer.send('flights_rt_api', flight_event)
    
    print(f"📡 Enviado Clima ({airport_update}) y Vuelo ({flight_id})")
    time.sleep(0.5)

producer.flush()
print("✅ Simulación finalizada.")