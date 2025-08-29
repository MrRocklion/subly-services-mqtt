import paho.mqtt.client as mqtt
import json
import requests
from dotenv import load_dotenv
import os

# Cargar variables desde .env
load_dotenv()
BROKER_URL = os.getenv("MQTT_BROKER")
BROKER_PORT = int(os.getenv("MQTT_PORT"))
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")
TOPIC = os.getenv("MQTT_TOPIC")
# Tópico donde publicaremos la respuesta de la API:
RESPONSE_TOPIC = os.getenv("MQTT_RESPONSE_TOPIC", f"{TOPIC}/response")

def api_query(data):
    """Llama a la API y devuelve un dict JSON para publicar por MQTT."""
    url = "http://localhost:8000/api/operations"
    try:
        r = requests.post(url, json=data, timeout=5)
        try:
            return r.json()  # ej: {"msg":"no existe ese usuario...","status":"error"}
        except ValueError:
            # Cuerpo no-JSON: lo empaquetamos
            return {
                "msg": r.text,
                "status": "error" if r.status_code >= 400 else "ok",
                "http_status": r.status_code,
            }
    except requests.RequestException as e:
        return {"msg": f"error llamando API: {e}", "status": "error"}

# --- Callbacks MQTT ---
def on_message(client, userdata, message):
    try:
        data_dict = json.loads(message.payload.decode())
    except json.JSONDecodeError:
        client.publish(RESPONSE_TOPIC, json.dumps({"msg": "mensaje no es JSON válido", "status": "error"}))
        return

    result = api_query(data_dict)
    # Publicamos SIEMPRE la respuesta de la API en RESPONSE_TOPIC
    client.publish(RESPONSE_TOPIC, json.dumps(result), qos=0, retain=False)
    print(f"[MQTT] publicado en {RESPONSE_TOPIC}: {result}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado al broker MQTT")
        client.subscribe(TOPIC)
        print(f"Suscrito al tópico: {TOPIC}")
    else:
        print(f"Error de conexión, código: {rc}")

# --- Cliente MQTT ---
client = mqtt.Client()  # si usas paho 2.x con callbacks v1: mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set()  # si tu broker es 1883 sin TLS, comenta esta línea

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_URL, BROKER_PORT, 60)
client.loop_forever()
