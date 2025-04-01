#!/usr/bin/env python
#
# sudo apt install python3-paho-mqtt
#
import random
import json
import influxdb
import yaml
import time
from paho.mqtt import client as mqtt_client

# Load configuration from config.yaml
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# EnergyBridge configuration
eb_config = config['energybridge']
hostname = eb_config['hostname']
(sub_hostname, junk) = hostname.split('.')  # Extract device name without domain
connect_hostname = eb_config['connect_hostname']
mqqt_port = eb_config['mqtt_port']
mqqt_topic = eb_config['mqtt_topic']

last_success = None  # Track last successful InfluxDB write
influx_client = None
client_id = f'publish-{random.randint(0, 1000)}'  # Unique client ID for MQTT

# example
#
# mosquitto_sub -h 192.168.4.49 -p 2883 -t '#' -v
#
#

def connect_mqtt() -> mqtt_client:
    def on_connect(one,two,three,four,five):
        print("on_connect()")
        print(one,two,three,four,five)

    client = mqtt_client.Client(client_id=client_id, clean_session=False, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)

    # client.username_pw_set(username, password)
    client.on_connect = on_connect

    print(f"attempting to connect to {connect_hostname}:{mqqt_port}")
    client.connect(connect_hostname, mqqt_port, keepalive=10)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print("on_message:userdata:", userdata)
        # Convert MQTT topic to InfluxDB measurement name
        oid = sub_hostname + '.' + msg.topic.replace('/', '.')
        payload = json.loads(msg.payload.decode())
        print(f"on_message decoded payload = {payload}")
        
        # DTE provides timestamps as Unix time_t (seconds since epoch)
        # We keep it in this format and let InfluxDB handle the precision
        timestamp = payload.get('time')
        now = time.time() * 1000000000  # Current time in nanoseconds for last_success tracking
        msg_type = payload.get('type', None)
        demand = payload.get('demand', 0)
        value = payload.get('value', 0)

        # Format data for InfluxDB line protocol
        # Format: measurement field=value timestamp
        if 'demand' in oid:
            server_data = f"{oid} value=%.2f {timestamp}\n" % demand
        else:
            server_data = f"{oid} value=%.2f {timestamp}\n" % value

        try:
            print(f"server_data={server_data}")
            # Write to InfluxDB using line protocol
            # time_precision='s' tells InfluxDB the timestamp is in seconds
            influx_client.write_points(server_data, protocol='line', time_precision='s')
            last_success = now
        except influxdb.exceptions.InfluxDBClientError as e:
             print(f"influx error is {e} with {server_data}")

    # subscribe to topics in mqqt_topic
    client.subscribe(mqqt_topic)
    # callback function
    client.on_message = on_message


def run():
    print("starting run")
    stay_running = True
    while stay_running:
        print("connect_mqtt")
        client = connect_mqtt()
        time.sleep(1)
        print("subscribe")
        subscribe(client)
        success = True
        client.loop_forever(timeout=60)

if __name__ == '__main__':
    # Initialize InfluxDB client with config values
    influx_client = influxdb.InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_username'],
        database=config['influx_db'],
        password=config['influx_db_pw'],
        ssl=False,
        verify_ssl=False
    )
    run()

