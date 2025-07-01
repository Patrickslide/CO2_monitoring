import paho.mqtt.client as mqtt
import json
from datetime import datetime
import requests
import sqlite3


DB_FILE = "timestamps_acembee.db"                                   #To store the timestamps.

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed_timestamps (timestamp TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def is_timestamp_processed(timestamp):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_timestamps WHERE timestamp = ?", (timestamp,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def save_timestamp(timestamp):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO processed_timestamps (timestamp) VALUES (?)", (timestamp,))
    conn.commit()
    conn.close()

init_db()

processed_timestamps = set()


influxdb_url = "https://eu-central-1-1.aws.cloud2.influxdata.com/api/v2/write"
influxdb_token = "1L9ERXh3UODzfFViOdtgsjBmiEOcvbEzjuatZTQDrHW2D7lS7g26GEQ2MccOYE-gewJ_J7BcR4S_EmjWNd2bcg=="  
influxdb_org = "clevertrack_API"  
influxdb_bucket = "acembee_API" 

headers = {
    "token": "f8eaf23b-ffc3-4318-845e-3479b2dd41d3",
    "Accept":"application/json"
}

influxdb_headers = {
    "Authorization": f"Token {influxdb_token}",
    "Content-Type": "text/plain; charset=utf-8",
    "Accept": "application/json",
}
influxdb_params = {
    "org": influxdb_org,
    "bucket": influxdb_bucket,
}


#Fix the time issue!!! Only last ones? or All of the same day??

broker_url = "mqtt.acembee.com"
receive_topics = [("49/+", 1)]                                             #Available topics:("49/+", 4), ("49/+", 1). Each sensor is a topic? Check it!
username = "FemerenÅU"  # Replace with username provided
password = "lt3zLEeJUP"  # Replace with password provided

# Connect to the MQTT broker
client = mqtt.Client(client_id="AU_CO2_project")  # give a short descriptive name of who you are
client.username_pw_set(username, password)

print(f"Connecting to MQTT broker at {broker_url}")

def on_connect(client, userdata, flags, rc):                    # on successful connection
    if rc == 0:
        print("Connected to MQTT broker")
        for topic in receive_topics:
            client.subscribe(topic, qos=1)
    
    else:
        print(f"Failed to connect, return code {rc}")

def write_to_influxdb(lines):
    if not lines:
        print("No data to write to InfluxDB.")
        return
    line_protocol_data = "\n".join(lines)
    print("Line Protocol Data:")
    print(line_protocol_data[:1000])  # Debugging: print first 1000 characters (since we write 10 rows each time).

    response = requests.post(
        influxdb_url, headers=influxdb_headers, params=influxdb_params, data=line_protocol_data
    )
    print("InfluxDB Response Status Code:", response.status_code)
    print("InfluxDB Response Text:", response.text) 
    if response.status_code == 204:
        print("Data written successfully to InfluxDB.")
    else:
        print(f"Failed to write data to InfluxDB: HTTP {response.status_code} - {response.text}")

def on_message(client, userdata, msg):                           # when a message is received.
    try:    
        payload = msg.payload.decode()
        print(f"---- Received message on topic {msg.topic}: {payload} \n")
        data = json.loads(payload)        
        print(data)
        label = data.get("label", "unknown")  # Measurement type (e.g., kwh)
        sensor_id = msg.topic.split("/")[-1]  # Extract the sensor ID from the topic
        timestamp = data.get("timestamp")                                            # All timestamps are two hours behind, beware!!!
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")            
        if is_timestamp_processed(timestamp):                                        # Ensure the timestamp is not already processed
            print(f"Timestamp {timestamp} already processed. Skipping.")
            return
        influx_data = []
        save_timestamp(timestamp)                                                    # Save timestamp to prevent re-processing. Do note that it's all 2 hours behind.
        for entry in data.get("data", []):                                                              #now it will be a list of tuples!
            offset, value = entry                                                                       #beware, now all must be added independently. The value is updated each time. 
            timestamp_unix = int(timestamp_dt.timestamp()* 1000000000)                                         #Reset
            timestamp_unix = timestamp_unix +  (offset* 1000000000)                                                               #Add the offset to adjust the timestamp.
            line = f"{label},sensor={sensor_id} offset={offset},value={value} {timestamp_unix}"
            influx_data.append(line)
        write_to_influxdb(influx_data)
    except Exception as e:
        print(f"Error processing message: {e}")



def on_disconnect(client, userdata, rc):                              # on disconnection
    print(f"Disconnected from MQTT broker with code {rc}")


client.on_connect = on_connect

client.on_message = on_message

client.on_disconnect = on_disconnect


client.connect(broker_url)
#client.loop_start()
client.loop_forever()      # Keep script running    


        # Iterate through "data" array and store each value


        # Write to InfluxDB
