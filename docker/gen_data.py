from kafka import KafkaProducer
import json
import time
import random

BROKERS = ['kafka:9093']
producer = KafkaProducer(bootstrap_servers=BROKERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Datos para simulación
CIUDADES_CLIMA = [
    {"name": "Sol", "airport_icao": "LEMD"},      # Madrid
    {"name": "Atlanta", "airport_icao": "KATL"},  # Atlanta
    {"name": "London", "airport_icao": "EGLL"}    # Londres
]

CONDICIONES = ["Clouds", "Clear", "Rain", "Thunderstorm", "Snow"]

print("🌪️ Generando datos formato REAL (OpenSky + OpenWeather)...")

for i in range(50):
    
    # --- 1. Generar Clima (Formato OpenWeather) ---
    ciudad = random.choice(CIUDADES_CLIMA)
    condicion_actual = random.choice(CONDICIONES)
    
    weather_payload = {
        "coord": {"lon": -3.69, "lat": 40.41},
        "weather": [{"id": 802, "main": condicion_actual, "description": "scattered clouds", "icon": "03d"}],
        "base": "stations",
        "main": {"temp": random.uniform(5, 30), "pressure": 1021, "humidity": 67},
        "visibility": 10000,
        "dt": int(time.time()),
        "name": ciudad["name"], # "Sol", "Atlanta"...
        "cod": 200
    }
    producer.send('weather_rt_api', weather_payload)

    # --- 2. Generar Vuelo (Formato OpenSky) ---
    # Nota: Simulamos que NiFi ha añadido 'delay_calculated'
    retraso = random.randint(-5, 60)
    
    # Caso de prueba: Si llueve en Madrid, poner retraso para ver si sale RIESGO ALTO
    if ciudad["name"] == "Sol" and condicion_actual == "Rain":
        retraso = 30 

    flight_payload = {
        "icao24": "ac52ff",
        "firstSeen": int(time.time()),
        "estDepartureAirport": ciudad["airport_icao"], # KATL, LEMD...
        "lastSeen": int(time.time()) + 100,
        "callsign": f"IBE{random.randint(100,999)} ", # Con espacio al final como el real
        "estDepartureAirportHorizDistance": 509,
        "departureAirportCandidatesCount": 317,
        # ESTE CAMPO LO DEBE AÑADIR NIFI (nosotros lo simulamos aquí)
        "delay_calculated": retraso 
    }

    # Introducir error de calidad a propósito (callsign vacío)
    if i % 20 == 0:
        flight_payload["callsign"] = "" 

    producer.send('flights_rt_api', flight_payload)
    
    print(f"📡 Enviado: Clima en {ciudad['name']} ({condicion_actual}) | Vuelo desde {ciudad['airport_icao']} (Retraso: {retraso})")
    time.sleep(0.5)

producer.flush()
print("✅ Simulación finalizada.")