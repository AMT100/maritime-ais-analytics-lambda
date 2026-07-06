import websocket
import json
import datetime
import os
import time
import uuid
 
# 1. Configuración
API_KEY = "216f9291ccbc87631ccd224eb3b6ccbef27a4e4a"
OUTPUT_DIR = "/home/ec2-user/datalake/bronze/stream_input"
 
# Asegurar que el directorio receptor existe
os.makedirs(OUTPUT_DIR, exist_ok=True)
 
# 2. Processamiento de mensajes en tiempo real
def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Capturar únicamente los reportes de posición, descartando los estáticos
        if data.get("MessageType") == "PositionReport":
            report = data["Message"]["PositionReport"]
            meta = data["MetaData"]
            
            # Mapear al esquema 
            spark_record = {
                "MMSI": str(meta.get("MMSI")),
                "BaseDateTime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "LAT": report.get("Latitude", 0.0),
                "LON": report.get("Longitude", 0.0),
                "SOG": report.get("Sog", 0.0),
                "COG": report.get("Cog", 0.0)
            }
            
            # Crear un nombre de archivo único para evitar sobrescrituras
            file_id = uuid.uuid4().hex[:8]
            final_path = f"{OUTPUT_DIR}/ais_{file_id}.json"
            temp_path = f"{final_path}.tmp"
            
            # Escribir un archivo temporal y luego renombrarlo para que Spark no intente 
            # leer el archivo mientras Python todavía lo está escribiendo.
            with open(temp_path, "w") as f:
                f.write(json.dumps(spark_record) + "\n")
            
            os.rename(temp_path, final_path)
            
            print(f"[+] Señal capturada -> MMSI: {spark_record['MMSI']} | Vel: {spark_record['SOG']} nudos")
            
    except Exception as e:
        pass # Ignorar los mensajes corruptos o incompletos
 
def on_error(ws, error):
    print(f"[!] Error de conexión: {error}")
 
def on_close(ws, close_status_code, close_msg):
    print("[-] Conexión cerrada con el servidor. Intentando reconectar en 5 segundos...")
    time.sleep(5)
    iniciar_conexion()
 
def on_open(ws):
    print("[✓] Conectado a AISStream.io.")
    print("[*] Suscribiéndose al cuadrante del Estrecho de Gibraltar...")
    
    # Payload de suscripción. Limitamos zona a (LatMin, LonMin, LatMax, LonMax)
    subscribe_message = {
        "APIKey": API_KEY,
        "BoundingBoxes": [[[35.8, -5.8], [36.2, -5.2]]],
        "FilterMessageTypes": ["PositionReport"]
    }
    ws.send(json.dumps(subscribe_message))
 
# 3. Bucle de ejecución
def iniciar_conexion():
    ws = websocket.WebSocketApp("wss://stream.aisstream.io/v0/stream",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
 
if __name__ == "__main__":
    iniciar_conexion()
