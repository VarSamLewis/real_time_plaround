from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import Flask
from diagrams.programming.language import Python
from diagrams.onprem.queue import Kafka
from diagrams.generic.storage import Storage
from diagrams.generic.device import Mobile
from diagrams.aws.analytics import DataPipeline
from diagrams.onprem.inmemory import Redis

# Create the diagram with better spacing
with Diagram("IoT Data Processing Pipeline", 
             show=False, 
             direction="TB",
             graph_attr={
                 "splines": "ortho",    # Orthogonal (right-angle) edges
                 "nodesep": "1.5",      # Horizontal spacing between nodes
                 "ranksep": "2.0",      # Vertical spacing between ranks/levels
                 "pad": "1.0",          # Padding around the entire graph
                 "bgcolor": "white"
             },
             node_attr={
                 "fontsize": "12",      # Font size for node labels
                 "margin": "0.3"        # Margin inside nodes
             },
             edge_attr={
                 "fontsize": "10"       # Font size for edge labels
             }):
    
    # Main program entry point
    main_program = Python("Main Program\n(Threading Control)")
    
    with Cluster("Data Generation Layer"):
        flask_server = Flask("Flask HTTP Server\n(Port 3030)")
        temp_model = Python("Temperature-Humidity\nModel Function")
        flask_server >> Edge(label="uses") >> temp_model
    
    with Cluster("Message Queue Layer"):
        mqtt_broker = Redis("MQTT Broker\n(localhost:1883)")
        raw_topic = Kafka("RawIOTData\nTopic")
        clean_topic = Kafka("CleanIOTData\nTopic")
        
        mqtt_broker >> raw_topic
        mqtt_broker >> clean_topic
    
    with Cluster("Data Processing Layer"):
        mqtt_producer = Python("MQTT Producer\n(Async Loop)")
        mqtt_consumer = Python("MQTT Consumer\n(Subscriber)")
        validator = DataPipeline("Data Validator\n& Structure")
        
        mqtt_consumer >> Edge(label="validates") >> validator
    
    with Cluster("Storage Layer"):
        csv_file = Storage("IOT_data.csv\n(CSV File Storage)")
        duplicate_checker = Python("Timestamp\nDuplicate Check")
        csv_writer = Python("CSV Writer\n(DictWriter)")
        
        validator >> duplicate_checker
        duplicate_checker >> csv_writer >> csv_file
    
    # Define the flow connections
    main_program >> Edge(label="starts threads") >> [flask_server, mqtt_producer, mqtt_consumer]
    
    # Data flow
    flask_server >> Edge(label="HTTP GET\n/iot_data") >> mqtt_producer
    mqtt_producer >> Edge(label="publish", style="bold") >> raw_topic
    raw_topic >> Edge(label="subscribe", style="bold") >> mqtt_consumer
    mqtt_consumer >> Edge(label="process") >> validator
    validator >> Edge(label="if valid &\nunique") >> csv_writer
    validator >> Edge(label="publish clean", style="dashed") >> clean_topic
    
    # Add IoT device representation
    iot_device = Mobile("Simulated IoT\nSensors (0-2)")
    iot_device >> Edge(label="simulated by", style="dotted") >> flask_server

# Additional diagram showing data structure with better spacing
with Diagram("Real Time Streaming (Current)", 
             show=True, 
             direction="LR",
             graph_attr={
                 "splines": "ortho",
                 "nodesep": "2.0",
                 "ranksep": "3.0",
                 "pad": "1.0",
                 "bgcolor": "white"
             }):
    
    raw_data = Python("Raw Data\n{\n  device_id: 0-2,\n  temperature: 900-1000,\n  humidity: calculated,\n  timestamp: ISO\n}")
    
    validation = DataPipeline("Validation Rules\n• Temp: -50 to 1500°C\n• Humidity: 0-100%\n• Valid timestamp\n• No duplicates")
    
    clean_data = Storage("Clean Data\n{\n  device_id: valid,\n  temperature: valid,\n  humidity: valid,\n  timestamp: unique\n}")
    
    error_data = Python("Error Data\n{\n  field: 'Error' or\n  'IOT Malfunctions'\n}")
    
    raw_data >> validation
    validation >> Edge(label="valid") >> clean_data
    validation >> Edge(label="invalid", color="red") >> error_data
