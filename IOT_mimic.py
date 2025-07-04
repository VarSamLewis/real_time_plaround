import random
import math
from datetime import datetime
from flask import Flask, jsonify
import json
import requests
import asyncio
import aiohttp
from dateutil import parser
from pyspark import SparkContext
import paho.mqtt.client as mqtt
import csv
import os
import threading
import json_utils
import types

# Ammednd this to a database eventually
csv_file_path = "IOT_data.csv"

csv_columns = [
    "device_id",
    "temperature",
    "humidity",
    "timestamp"
]
    
app = Flask(__name__)

# MQTT Broker details
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
RAW_TOPIC = "RawIOTData"
CLEAN_TOPIC = "CleanIOTData"

def model_temp_humidity(temp_celsius: float, a: float = 100, b: float = 0.05) -> tuple[float, float]:
    """
    Maps temperature to humidity using an exponential decay.
    """
    try:
        humidity = a * math.exp(-b * temp_celsius)
        return temp_celsius, humidity
    except Exception as e:
        print(f"Error computing humidity: {e}")
        return temp_celsius, 0.0

@app.route('/iot_data')
def get_message():
    """
    Flask route to generate simulated IoT data as JSON.
    """
    device_id = random.randint(0, 2)
    temp = random.uniform(0.9, 1.0) * 1000
    _, humidity = model_temp_humidity(temp)
    
    data = {
        'device_id': device_id,
        'temperature': round(temp, 2),
        'humidity': round(humidity, 2),
        'timestamp': datetime.now().isoformat()
    }
    return jsonify(data)

def start_flask():
    app.run(host='127.0.0.1', port=3030, debug=False, use_reloader=False)


def get_iot_data_stream(url):
    """
    Fetch data from the IoT HTTP endpoint.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error in connection: {e}")
        return None


def write_to_csv(data, file_path=csv_file_path):
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        if not file_exists:
            writer.writeheader()  # write header only once
        # Write the row with keys matching CSV columns
        writer.writerow({
            'timestamp': data.get('timestamp', ''),
            'device_id': data.get('device_id', ''),
            'temperature': data.get('temperature', ''),
            'humidity': data.get('humidity', '')
        })

        
def timestamp_exist(timestamp, file_path=csv_file_path):
    """
    Check if a timestamp already exists in the CSV file to avoid duplicates.
    """
    if not os.path.isfile(file_path):
        return False  # CSV doesn't exist yet, so timestamp can't exist
    
    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get('timestamp') == timestamp:
                return True
    return False

def structure_validate_data(msg):
    """
    Validate and structure incoming Kafka message .
    """
    data_dict = {}
    try:
        msg_str = msg.value.decode('utf-8')
        data = json.loads(msg_str)
    except Exception as e:
        print(f"Error decoding JSON: {e}")
        return None

    try:
        # Validate timestamp
        timestamp = parser.isoparse(data.get('timestamp'))
        data_dict['timestamp'] = timestamp.isoformat()
    except Exception as e:
        data_dict['timestamp'] = "Error"

    try:
        temp = float(data.get('temperature'))
        # Check plausible temperature range, adjust as needed
        if temp < -50 or temp > 1500:
            data_dict['temperature'] = "IOT Malfunctions"
        else:
            data_dict['temperature'] = temp
    except Exception:
        data_dict['temperature'] = "Error"

    try:
        humidity = float(data.get('humidity'))
        if humidity < 0 or humidity > 100:
            data_dict['humidity'] = "IOT Malfunctions"
        else:
            data_dict['humidity'] = humidity
    except Exception:
        data_dict['humidity'] = "Error"

    try:
        device_id = int(data.get('device_id'))
        data_dict['device_id'] = device_id
    except Exception:
        data_dict['device_id'] = "Error"

    return data_dict

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(RAW_TOPIC)

def on_message(client, userdata, msg):
    """
    This will be the MQTT subscriber callback, similar to kafka_consumer_loop.
    """
    try:
        message = msg.payload.decode('utf-8')
        if message != "Error in Connection":
            data = structure_validate_data(type('Msg', (object,), {'value': msg.payload})())
            if data and data.get('timestamp') != "Error" and not timestamp_exist(data['timestamp']):
                write_to_csv(data)
                # Publish cleaned data
                #client.publish(CLEAN_TOPIC, json.dumps(data, default=json_utils.default))
                client.publish(CLEAN_TOPIC, json.dumps(data))
            print(data)
    except Exception as e:
        print(f"Error processing MQTT message: {e}")

def mqtt_consumer_loop():
    """
    MQTT subscriber loop runs in a thread.
    """
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        return

    client.loop_forever()

async def mqtt_producer_loop():
    """
    Async function to fetch from HTTP endpoint and publish to MQTT.
    """
    client = mqtt.Client()
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        return
    
    client.loop_start()  # start background thread to handle network traffic
    
    url = 'http://localhost:3030/iot_data'
    start_time = asyncio.get_event_loop().time()
    max_duration = 600  # 10 mins
    
    async with aiohttp.ClientSession() as session:
        try:
            while True:
                try:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        msg = await response.text()
                    client.publish(RAW_TOPIC, msg)
                    print(f"Published message: {msg[:50]}...")
                except Exception as e:
                    print(f"Error fetching/publishing data: {e}")

                await asyncio.sleep(1)

                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > max_duration:
                    print("Reached max duration, stopping producer loop.")
                    break
        finally:
            client.loop_stop()
            client.disconnect()



if __name__ == "__main__":
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()

    # Start MQTT consumer (subscriber) in a thread
    consumer_thread = threading.Thread(target=mqtt_consumer_loop, daemon=True)
    consumer_thread.start()

    # Start MQTT producer (publisher) loop
    asyncio.run(mqtt_producer_loop())


