#!/usr/bin/env python
#
# sudo apt install python3-paho-mqtt
#
import random
import json
import influxdb
import yaml
import time
import os
import logging
from paho.mqtt import client as mqtt_client
from collections import deque
from datetime import datetime
from typing import Optional, Tuple

# Load configuration from config.yaml
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    file_path = os.path.dirname(__file__)
#    print(file_path)
    os.chdir(file_path)
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

# EnergyBridge configuration
eb_config = config['energybridge']
hostname = eb_config['hostname']
(sub_hostname, junk) = hostname.split('.')  # Extract device name without domain
connect_hostname = eb_config['connect_hostname']
mqqt_port = eb_config['mqtt_port']
mqqt_topic = eb_config['mqtt_topic']

# Initialize backlog queue without size limit
backlog_queue = deque()
last_success = None  # Track last successful InfluxDB write
influx_client = None
client_id = f'publish-{random.randint(0, 1000)}'  # Unique client ID for MQTT

# Connection health and retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 60.0  # seconds
BACKOFF_MULTIPLIER = 2.0
CIRCUIT_BREAKER_THRESHOLD = 10  # consecutive failures before circuit opens
CIRCUIT_BREAKER_TIMEOUT = 300  # seconds before trying again
HEALTH_CHECK_INTERVAL = 30  # seconds between health checks

# MQTT timeout and reconnection configuration (can be overridden by config)
MQTT_MESSAGE_TIMEOUT = eb_config.get('mqtt_message_timeout', 300)  # seconds without messages before considering connection lost
MQTT_RECONNECT_DELAY = eb_config.get('mqtt_reconnect_delay', 5)  # seconds to wait before attempting reconnection
MQTT_MAX_RECONNECT_ATTEMPTS = eb_config.get('mqtt_max_reconnect_attempts', 10)  # maximum reconnection attempts before giving up
MQTT_HEALTH_CHECK_INTERVAL = eb_config.get('mqtt_health_check_interval', 60)  # seconds between MQTT health checks

# Circuit breaker state
circuit_breaker_failures = 0
circuit_breaker_last_failure = 0
circuit_breaker_open = False

# MQTT connection state
mqtt_client = None
mqtt_last_message_time = None
mqtt_connected = False
mqtt_reconnect_attempts = 0
mqtt_last_health_check = 0

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dte_energy_bridge.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# example
#
# mosquitto_sub -h 192.168.4.49 -p 2883 -t '#' -v
#
#

def check_circuit_breaker() -> bool:
    """Check if circuit breaker is open and should block requests"""
    global circuit_breaker_open, circuit_breaker_failures, circuit_breaker_last_failure
    
    if circuit_breaker_open:
        # Check if enough time has passed to try again
        if time.time() - circuit_breaker_last_failure > CIRCUIT_BREAKER_TIMEOUT:
            logger.info("Circuit breaker timeout expired, attempting to close circuit")
            circuit_breaker_open = False
            circuit_breaker_failures = 0
            return False
        return True
    return False

def record_success():
    """Record a successful operation and reset circuit breaker"""
    global circuit_breaker_failures, circuit_breaker_open
    circuit_breaker_failures = 0
    circuit_breaker_open = False

def record_failure():
    """Record a failed operation and potentially open circuit breaker"""
    global circuit_breaker_failures, circuit_breaker_last_failure, circuit_breaker_open
    circuit_breaker_failures += 1
    circuit_breaker_last_failure = time.time()
    
    if circuit_breaker_failures >= CIRCUIT_BREAKER_THRESHOLD:
        circuit_breaker_open = True
        logger.warning(f"Circuit breaker opened after {circuit_breaker_failures} consecutive failures")

def exponential_backoff_delay(attempt: int) -> float:
    """Calculate exponential backoff delay with jitter"""
    delay = min(INITIAL_RETRY_DELAY * (BACKOFF_MULTIPLIER ** attempt), MAX_RETRY_DELAY)
    # Add jitter to prevent thundering herd
    jitter = random.uniform(0.1, 0.3) * delay
    return delay + jitter

def is_retryable_error(error: Exception) -> bool:
    """Determine if an error is retryable"""
    if isinstance(error, influxdb.exceptions.InfluxDBServerError):
        error_str = str(error).lower()
        # Retry on timeout, temporary server errors, but not on authentication/permission errors
        return any(keyword in error_str for keyword in ['timeout', 'temporary', 'unavailable', 'connection'])
    elif isinstance(error, influxdb.exceptions.InfluxDBClientError):
        error_str = str(error).lower()
        # Retry on connection issues, but not on data format errors
        return any(keyword in error_str for keyword in ['connection', 'timeout', 'network'])
    return False

def write_to_influxdb_with_retry(data: str) -> bool:
    """Write data to InfluxDB with retry logic and circuit breaker"""
    global last_success
    
    if influx_client is None:
        logger.error("InfluxDB client is not initialized, adding data to backlog")
        backlog_queue.append(data)
        return False
    
    # Check circuit breaker first
    if check_circuit_breaker():
        logger.warning("Circuit breaker is open, adding data to backlog")
        backlog_queue.append(data)
        return False
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.debug(f"Attempting to write to InfluxDB (attempt {attempt + 1}/{MAX_RETRIES + 1})")
            influx_client.write_points(data, protocol='line', time_precision='ms')
            last_success = time.time() * 1000000000
            record_success()
            logger.debug("Successfully wrote to InfluxDB")
            return True
            
        except Exception as e:
            logger.warning(f"InfluxDB write attempt {attempt + 1} failed: {e}")
            
            if not is_retryable_error(e):
                logger.error(f"Non-retryable error: {e}")
                record_failure()
                backlog_queue.append(data)
                return False
            
            if attempt < MAX_RETRIES:
                delay = exponential_backoff_delay(attempt)
                logger.info(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All {MAX_RETRIES + 1} attempts failed")
                record_failure()
                backlog_queue.append(data)
                return False
    
    return False

def check_influxdb_health() -> bool:
    """Check if InfluxDB is healthy by attempting a simple query"""
    if influx_client is None:
        logger.warning("InfluxDB client is not initialized")
        return False
        
    try:
        # Try to ping the database
        influx_client.ping()
        return True
    except Exception as e:
        logger.warning(f"InfluxDB health check failed: {e}")
        return False

def check_mqtt_health() -> bool:
    """Check if MQTT connection is healthy based on recent message activity"""
    global mqtt_last_message_time, mqtt_connected
    
    if mqtt_client is None:
        logger.warning("MQTT client is not initialized")
        return False
    
    if not mqtt_connected:
        logger.warning("MQTT client is not connected")
        return False
    
    # Check if we've received messages recently
    if mqtt_last_message_time is None:
        logger.warning("No MQTT messages received yet")
        return False
    
    time_since_last_message = time.time() - mqtt_last_message_time
    if time_since_last_message > MQTT_MESSAGE_TIMEOUT:
        logger.warning(f"No MQTT messages received for {time_since_last_message:.1f} seconds (timeout: {MQTT_MESSAGE_TIMEOUT}s)")
        return False
    
    return True

def reconnect_mqtt() -> bool:
    """Attempt to reconnect to MQTT broker"""
    global mqtt_client, mqtt_connected, mqtt_reconnect_attempts, mqtt_last_message_time
    
    try:
        logger.info(f"Attempting to reconnect to MQTT broker (attempt {mqtt_reconnect_attempts + 1}/{MQTT_MAX_RECONNECT_ATTEMPTS})")
        
        # Disconnect existing client if any
        if mqtt_client is not None:
            try:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            except:
                pass
        
        # Create new client
        mqtt_client = connect_mqtt()
        time.sleep(1)
        
        # Subscribe to topics
        subscribe(mqtt_client)
        
        # Start the loop
        mqtt_client.loop_start()
        
        # Reset connection state
        mqtt_connected = True
        mqtt_last_message_time = time.time()  # Reset message timer
        mqtt_reconnect_attempts = 0
        
        logger.info("Successfully reconnected to MQTT broker")
        return True
        
    except Exception as e:
        logger.error(f"Failed to reconnect to MQTT broker: {e}")
        mqtt_reconnect_attempts += 1
        mqtt_connected = False
        return False

def reconnect_influxdb() -> bool:
    """Attempt to reconnect to InfluxDB"""
    global influx_client
    try:
        logger.info("Attempting to reconnect to InfluxDB...")
        influx_client = influxdb.InfluxDBClient(
            host=config['influx_host'],
            port=config['influx_port'],
            username=config['influx_username'],
            database=config['influx_db'],
            password=config['influx_db_pw'],
            ssl=False,
            verify_ssl=False,
            timeout=30  # Add timeout for connection attempts
        )
        
        # Test the connection
        if check_influxdb_health():
            logger.info("Successfully reconnected to InfluxDB")
            record_success()
            return True
        else:
            logger.error("InfluxDB reconnection failed health check")
            return False
            
    except Exception as e:
        logger.error(f"Failed to reconnect to InfluxDB: {e}")
        record_failure()
        return False

def process_backlog():
    """Process any backlogged points when connection is restored"""
    global last_success
    if not backlog_queue:
        return
    
    logger.info(f"Processing backlog of {len(backlog_queue)} points")
    processed_count = 0
    failed_count = 0
    
    while backlog_queue:
        point = backlog_queue.popleft()
        
        # Use the new retry logic for backlog processing
        if write_to_influxdb_with_retry(point):
            processed_count += 1
        else:
            # If we fail, put the point back at the front of the queue
            backlog_queue.appendleft(point)
            failed_count += 1
            logger.warning(f"Failed to process backlog point, {len(backlog_queue)} points remaining")
            break
    
    if processed_count > 0:
        logger.info(f"Successfully processed {processed_count} backlog points")
    if failed_count > 0:
        logger.warning(f"Failed to process {failed_count} backlog points, will retry later")

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, reason_code, properties):
        global mqtt_connected, mqtt_last_message_time
        if reason_code == 0:
            logger.info("Successfully connected to MQTT broker")
            mqtt_connected = True
            mqtt_last_message_time = time.time()  # Initialize message timer
        else:
            logger.error(f"Failed to connect to MQTT broker: {reason_code}")
            mqtt_connected = False

    def on_disconnect(client, userdata, flags, reason_code, properties):
        global mqtt_connected
        logger.warning(f"Disconnected from MQTT broker: {reason_code}")
        mqtt_connected = False

    client = mqtt_client.Client(client_id=client_id, clean_session=False, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)

    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    logger.info(f"Attempting to connect to {connect_hostname}:{mqqt_port}")
    client.connect(connect_hostname, mqqt_port, keepalive=10)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global mqtt_last_message_time
        # Update message timestamp for health monitoring
        mqtt_last_message_time = time.time()
        
        logger.debug(f"Received MQTT message on topic: {msg.topic}")
        # Convert MQTT topic to InfluxDB measurement name
        oid = sub_hostname + '.' + msg.topic.replace('/', '.')
        payload = json.loads(msg.payload.decode())
        logger.debug(f"Decoded payload = {payload}")
        
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

        logger.debug(f"server_data={server_data}")
        
        # Use the new retry logic for writing to InfluxDB
        if write_to_influxdb_with_retry(server_data):
            # If we have a successful write, try to process any backlog
            if backlog_queue:
                process_backlog()
        else:
            logger.warning(f"Failed to write data point, added to backlog. Current backlog size: {len(backlog_queue)}")

    # subscribe to topics in mqqt_topic
    client.subscribe(mqqt_topic)
    # callback function
    client.on_message = on_message


def health_monitor():
    """Background health monitoring for InfluxDB and MQTT connections"""
    last_health_check = 0
    last_mqtt_health_check = 0
    
    while True:
        current_time = time.time()
        
        # Check InfluxDB health periodically
        if current_time - last_health_check > HEALTH_CHECK_INTERVAL:
            if not check_influxdb_health():
                logger.warning("InfluxDB health check failed, attempting reconnection")
                if reconnect_influxdb():
                    # Try to process backlog after successful reconnection
                    if backlog_queue:
                        logger.info("Attempting to process backlog after reconnection")
                        process_backlog()
            last_health_check = current_time
        
        # Check MQTT health periodically
        if current_time - last_mqtt_health_check > MQTT_HEALTH_CHECK_INTERVAL:
            if not check_mqtt_health():
                logger.warning("MQTT health check failed, attempting reconnection")
                if mqtt_reconnect_attempts < MQTT_MAX_RECONNECT_ATTEMPTS:
                    if reconnect_mqtt():
                        logger.info("MQTT reconnection successful")
                    else:
                        logger.error(f"MQTT reconnection failed (attempt {mqtt_reconnect_attempts}/{MQTT_MAX_RECONNECT_ATTEMPTS})")
                        time.sleep(MQTT_RECONNECT_DELAY)
                else:
                    logger.error("Maximum MQTT reconnection attempts reached, giving up")
            last_mqtt_health_check = current_time
        
        time.sleep(5)  # Check every 5 seconds

def run():
    global mqtt_client
    logger.info("Starting DTE Energy Bridge")
    
    # Start health monitoring in a separate thread
    import threading
    health_thread = threading.Thread(target=health_monitor, daemon=True)
    health_thread.start()
    
    stay_running = True
    while stay_running:
        try:
            logger.info("Connecting to MQTT broker")
            mqtt_client = connect_mqtt()
            time.sleep(1)
            logger.info("Subscribing to MQTT topics")
            subscribe(mqtt_client)
            
            # Use loop_start for non-blocking operation
            mqtt_client.loop_start()
            
            # Keep the main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down gracefully")
            stay_running = False
            if mqtt_client is not None:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    try:
        # Initialize InfluxDB client with config values
        logger.info("Initializing InfluxDB client...")
        influx_client = influxdb.InfluxDBClient(
            host=config['influx_host'],
            port=config['influx_port'],
            username=config['influx_username'],
            database=config['influx_db'],
            password=config['influx_db_pw'],
            ssl=False,
            verify_ssl=False,
            timeout=30  # Add timeout for connection attempts
        )
        
        # Test initial connection
        if check_influxdb_health():
            logger.info("Initial InfluxDB connection successful")
        else:
            logger.warning("Initial InfluxDB connection failed, will retry during operation")
        
        run()
        
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        exit(1)

