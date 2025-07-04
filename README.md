# IoT Data Pipeline (Prototype)

A lightweight Python prototype for simulating, processing, and storing IoT sensor data using Flask, MQTT, and CSV.

## What It Does

This project demonstrates a basic IoT data pipeline that:

- Simulates temperature and humidity sensor data
- Sends data via MQTT messaging
- Validates and deduplicates incoming data
- Stores clean results in a CSV file

## Components

- **Flask Server** (`/iot_data`, port 3030): Simulates sensor data
- **MQTT Producer**: Fetches data from Flask and publishes to `RawIOTData`
- **MQTT Consumer**: Validates and republishes clean data to `CleanIOTData`
- **CSV Storage**: Appends validated, deduplicated data to `IOT_data.csv`

## Validation Rules

- Temperature: -50°C to 1500°C
- Humidity: 0% to 100% (based on exponential decay model)
- Timestamp: ISO format, no duplicates
- Device ID: Must be an integer

Invalid or malformed data is flagged accordingly.

## Dependencies

```bash
pip install flask requests aiohttp python-dateutil paho-mqtt
```

Steps to run the app:
	1. Start an MQTT broker (e.g. Mosquitto)
	2. Run the pipeline:
```
python IOT_mimic.py
```
This will:

	Launch the Flask server
	Start MQTT consumer in a background thread
	Begin the producer loop (1 hour duration)

## Output

IOT_data.csv: Clean sensor data

Console logs: Processing status and errors

MQTT topics: RawIOTData and CleanIOTData

## Future Plans

Replace CSV with PostgreSQL or MongoDB
Add MQTT authentication and TLS
Improve error handling and observability
Support horizontal scaling and persistence
Add spark streaming for real-time processing
Create plotly dashboard for data visualization
Learn how to deploy to a WSGI server like Gunicorn or uWSGI

## Note
This is a prototype for learning and experimentation. Contributions and suggestions are welcome as we explore how to make it more robust and production-ready.