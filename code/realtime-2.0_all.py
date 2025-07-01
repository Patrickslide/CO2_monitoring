import requests
from datetime import datetime, time
from time import sleep
import json
import xml.etree.ElementTree as ET
import sqlite3

DB_FILE = "timestamps.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS processed_timestamps (timestamp TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def save_timestamp(timestamp):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO processed_timestamps (timestamp) VALUES (?)", (timestamp,))
    conn.commit()
    conn.close()

influxdb_url = "https://eu-central-1-1.aws.cloud2.influxdata.com/api/v2/write"
influxdb_token = "1L9ERXh3UODzfFViOdtgsjBmiEOcvbEzjuatZTQDrHW2D7lS7g26GEQ2MccOYE-gewJ_J7BcR4S_EmjWNd2bcg=="
influxdb_org = "clevertrack_API"
influxdb_bucket = "clevertrack_API"
influxdb_headers = {"Authorization": f"Token {influxdb_token}", "Content-Type": "text/plain; charset=utf-8", "Accept": "application/json"}
influxdb_params = {"org": influxdb_org, "bucket": influxdb_bucket,}

configurations = [
    {
        "name": "First Source",
        "api_token": "3543082c-b5de-4b3b-9145-18594e2c9aa6",
        "id_list": [16671, 16695, 16696, 16102, 16655, 16698, 16615]
    },
    {
        "name": "Second Source",
        "api_token": "f8eaf23b-ffc3-4318-845e-3479b2dd41d3",
    }]

processed_timestamps = set()

def fetch_data(date, token):
    url = "https://ctpublic.azurewebsites.net/api/vehicles"
    headers = {"token": token, "Accept": "application/json"}
    print("Fetching {}'s Vehicle Data".format(date))
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print("Error parsing JSON:", e)
    else:
        print(f"Failed to fetch vehicles: HTTP {response.status_code} - {response.text}")
    return None

def write_to_influxdb(lines):
    if not lines:
        print("No data to write to InfluxDB.")
        return

    line_protocol_data = "\n".join(lines)
    print("Sending Line Protocol to InfluxDB...")
    response = requests.post(influxdb_url, headers=influxdb_headers,
                             params=influxdb_params, data=line_protocol_data)
    print("InfluxDB Response Status:", response.status_code)
    print("InfluxDB Response Text:", response.text)
    if response.status_code == 204:
        print("Data written successfully.")
    else:
        print("Failed to write to InfluxDB.")

def set_influxdb_lines(data, date, token, id_filter=None):
    lines = []
    url = "https://ctpublic.azurewebsites.net/api/devicelogs"
    date1 = datetime.combine(date, time.min).strftime('%Y-%m-%dT%X')
    date2 = datetime.combine(date, time.max).strftime('%Y-%m-%dT%X')

    for item in data:
        if "id" in item:
            machine_id = item["id"]
            machine_name = item["name"]

        if id_filter and machine_id not in id_filter:
            continue

        headers2 = {
            "vehicle": str(machine_id),
            "start": date1,
            "stop": date2,
            "token": token
        }
        url2 = f"{url}?vehicle={machine_id}&start={date1}&stop={date2}"
        response2 = requests.get(url2, headers=headers2)

        if response2.status_code != 200:
            print(f"Failed to fetch logs for {machine_name}: {response2.status_code}")
            continue

        try:
            root = ET.fromstring(response2.text)
            logs = root.find("logs")
            new_entries = []

            for log in logs:
                log_data = {child.tag: child.text for child in log}
                timestamp_str = log_data.get("timestamp", "N/A")
                if timestamp_str == "N/A":
                    continue

                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                if timestamp in processed_timestamps:
                    continue

                save_timestamp(timestamp_str)
                processed_timestamps.add(timestamp)
                new_entries.append(log_data)

            for line in new_entries:
                lat = line.get("lat", "N/A")
                lon = line.get("lon", "N/A")
                speed = line.get("speed", 0)
                engine_hours_tot = line.get("engineHourTot", 0)
                engine_hours_cnt = line.get("engineHourCnt", 0)
                distance = line.get("distance", 0)
                distance_tot = line.get("distanceTot", 0)
                voltage = line.get("externalVolt", 0)
                rpm = line.get("rpm", 0)
                fuel_tot = line.get("fuelTot", 0)
                fuel_cnt = line.get("fuelCnt", 0)
                fuelLev = line.get("fuelLev", 0)
                temp = line.get("engineTemp", 0)
                engine_load = line.get("engineLoad", 0)

                newline = (
                    f"vehicle_data,machine={machine_name.replace(' ', '')} "
                    f"engine_hours_tot={engine_hours_tot},engine_hours_cnt={engine_hours_cnt},lat={lat},lon={lon},speed={speed},"
                    f"distance={distance},distance_tot={distance_tot},Voltage={voltage},RPM={rpm},fuel_tot={fuel_tot},"
                    f"fuel_count={fuel_cnt},fuel_level={fuelLev},Temperature={temp},engine_load={engine_load} "
                    f"{int(timestamp.timestamp() * 1e9)}"
                )
                lines.append(newline)

        except ET.ParseError as e:
            print("XML parsing error:", e)
    return lines

def main():
    init_db()
    while True:
        start_time = datetime.now()
        now = datetime.now().replace(second=0, microsecond=0)

        for config in configurations:
            print(f"Processing {config['name']}")
            vehicle_data = fetch_data(now, config["api_token"])
            if not vehicle_data:
                print("No data fetched.")
                continue

            influx_lines = set_influxdb_lines(vehicle_data, now, config["api_token"], config.get("id_list"))
            write_to_influxdb(influx_lines)

        time_frame = (datetime.now() - start_time).total_seconds()
        wait_time = max(0, 45 - time_frame)
        print(f"Waiting {wait_time:.2f} seconds...\n")
        sleep(wait_time)

if __name__ == "__main__":
    main()