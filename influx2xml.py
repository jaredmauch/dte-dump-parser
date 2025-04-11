#!/usr/bin/env python3

"""
InfluxDB to ESPI XML Converter

This script reads energy usage data from InfluxDB and converts it to the ESPI
(Energy Service Provider Interface) XML format. It uses the EnergyBridge data
stored in InfluxDB and generates XML files according to the ESPI schema.
"""

import argparse
import yaml
from datetime import datetime, timedelta, timezone, UTC
import xml.etree.ElementTree as ET
from influxdb import InfluxDBClient
import pytz
import os

def load_config(config_file: str) -> dict:
    """Load configuration from YAML file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dictionary containing configuration values
    """
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def connect_to_influx(config: dict) -> InfluxDBClient:
    """Connect to InfluxDB using configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        InfluxDBClient instance
    """
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_username'],
        password=config['influx_db_pw'],
        database=config['influx_db']
    )

def get_energy_data(
    client: InfluxDBClient,
    start_time: datetime,
    end_time: datetime,
    device_id: str = "energybridge2-2c1999d4e6b58379"
) -> list:
    """Retrieve energy usage data from InfluxDB.
    
    Args:
        client: InfluxDBClient instance
        start_time: Start time for data retrieval (UTC)
        end_time: End time for data retrieval (UTC)
        device_id: EnergyBridge device ID to query
        
    Returns:
        List of energy usage records
    """
    print(f"Querying InfluxDB from {start_time} to {end_time} UTC")
    print(f"Using device: {device_id}")
    
    # Query to get energy usage data
    query = f"""
    SELECT value, time
    FROM "{device_id}.event.metering.summation.minute"
    WHERE time >= '{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    AND time <= '{end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    ORDER BY time
    """
    
    print(f"\nExecuting query: {query}")
    result = client.query(query)
    points = list(result.get_points())
    print(f"Found {len(points)} data points")
    
    if points:
        print("\nFirst few data points:")
        for point in points[:3]:
            print(f"- Time: {point['time']}, Value: {point['value']}")
    
    return points

def create_espi_xml(
    energy_data: list,
    start_time: datetime,
    end_time: datetime,
    output_file: str
) -> None:
    """Create ESPI XML file from energy data.
    
    Args:
        energy_data: List of energy usage records
        start_time: Start time of the data
        end_time: End time of the data
        output_file: Path to output XML file
    """
    # Create root feed element with proper namespaces
    root = ET.Element('feed')
    root.set('xmlns', 'http://www.w3.org/2005/Atom')
    root.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    root.set('xsi:schemaLocation', 'http://naesb.org/espi espiDerived.xsd')
    
    # Add feed metadata
    feed_id = ET.SubElement(root, 'id')
    feed_id.text = 'urn:uuid:00000000-0000-0000-0000-000000000001'
    
    title = ET.SubElement(root, 'title')
    title.text = 'DTE Energy Usage Data'
    
    updated = ET.SubElement(root, 'updated')
    updated.text = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Add UsagePoint entry
    entry = ET.SubElement(root, 'entry')
    entry_id = ET.SubElement(entry, 'id')
    entry_id.text = 'urn:uuid:00000000-0000-0000-0000-000000000002'
    
    link = ET.SubElement(entry, 'link')
    link.set('href', 'UsagePoint/01')
    link.set('rel', 'self')
    
    title = ET.SubElement(entry, 'title')
    title.text = 'Electric Data'
    
    published = ET.SubElement(entry, 'published')
    published.text = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    updated = ET.SubElement(entry, 'updated')
    updated.text = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    content = ET.SubElement(entry, 'content')
    usage_point = ET.SubElement(content, 'UsagePoint')
    usage_point.set('xmlns', 'http://naesb.org/espi')
    
    service_category = ET.SubElement(usage_point, 'ServiceCategory')
    kind = ET.SubElement(service_category, 'kind')
    kind.text = '0'  # electricity
    
    # Add IntervalBlock entry
    entry = ET.SubElement(root, 'entry')
    entry_id = ET.SubElement(entry, 'id')
    entry_id.text = 'urn:uuid:00000000-0000-0000-0000-000000000003'
    
    link = ET.SubElement(entry, 'link')
    link.set('href', 'IntervalBlock/01')
    link.set('rel', 'self')
    
    title = ET.SubElement(entry, 'title')
    title.text = 'Electric readings'
    
    published = ET.SubElement(entry, 'published')
    published.text = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    updated = ET.SubElement(entry, 'updated')
    updated.text = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    content = ET.SubElement(entry, 'content')
    interval_block = ET.SubElement(content, 'IntervalBlock')
    interval_block.set('xmlns', 'http://naesb.org/espi')
    
    # Add interval
    interval = ET.SubElement(interval_block, 'interval')
    duration = ET.SubElement(interval, 'duration')
    duration.text = '86400'  # 1 day in seconds
    start = ET.SubElement(interval, 'start')
    start.text = str(int(start_time.timestamp()))
    
    # Add interval readings
    for record in energy_data:
        reading = ET.SubElement(interval_block, 'IntervalReading')
        
        # Add time period
        time_period = ET.SubElement(reading, 'timePeriod')
        time_start = ET.SubElement(time_period, 'start')
        dt = datetime.strptime(record['time'], '%Y-%m-%dT%H:%M:%SZ')
        time_start.text = str(int(dt.timestamp()))
        time_duration = ET.SubElement(time_period, 'duration')
        time_duration.text = '3600'  # 1 hour
        
        # Add value (convert kW to W)
        value = ET.SubElement(reading, 'value')
        value.text = str(int(record['value'] * 1000))  # Convert to watt-hours
    
    # Add XML stylesheet reference
    pi = ET.ProcessingInstruction('xml-stylesheet', 'type="text/xsl" href="GreenButtonDataStyleSheet.xslt"')
    root.insert(0, pi)
    
    # Create XML tree and write to file with proper formatting
    tree = ET.ElementTree(root)
    ET.indent(tree)  # Pretty print the XML
    
    # Write with XML declaration and proper encoding
    with open(output_file, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding='utf-8')

def check_data_availability(client: InfluxDBClient, device_id: str) -> None:
    """Check what data is available in the database.
    
    Args:
        client: InfluxDBClient instance
        device_id: EnergyBridge device ID to query
    """
    # Check first and last timestamps
    query = f"""
    SELECT value, time
    FROM "{device_id}.event.metering.summation.minute"
    ORDER BY time DESC
    LIMIT 1
    """
    
    result = client.query(query)
    points = list(result.get_points())
    
    if points:
        point = points[0]
        print(f"\nData availability for {device_id}:")
        print(f"Latest timestamp: {point['time']}")
        print(f"Latest value: {point['value']}")
        
        # Get count of records
        count_query = f"""
        SELECT COUNT(value)
        FROM "{device_id}.event.metering.summation.minute"
        """
        count_result = client.query(count_query)
        count_points = list(count_result.get_points())
        if count_points:
            print(f"Total records: {count_points[0]['count']}")
    else:
        print(f"\nNo data found for {device_id}")

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Convert InfluxDB energy data to ESPI XML format.'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--start',
        help='Start time in ISO format UTC (e.g. 2024-03-01T00:00:00Z)'
    )
    parser.add_argument(
        '--end',
        help='End time in ISO format UTC (e.g. 2024-03-31T23:59:59Z)'
    )
    parser.add_argument(
        '--output',
        default='energy_data.xml',
        help='Output XML file path (default: energy_data.xml)'
    )
    parser.add_argument(
        '--device',
        default='energybridge2-2c1999d4e6b58379',
        help='EnergyBridge device ID to query (default: energybridge2-2c1999d4e6b58379)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed processing information'
    )
    parser.add_argument(
        '--check-data',
        action='store_true',
        help='Check available data ranges without generating XML'
    )
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Set up time range in UTC
    end_time = datetime.now(UTC)
    if args.end:
        end_time = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ")
    
    if args.start:
        start_time = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ")
    else:
        start_time = end_time - timedelta(days=30)
    
    print(f"Connecting to InfluxDB at {config['influx_host']}:{config['influx_port']}")
    
    # Connect to InfluxDB
    client = connect_to_influx(config)
    
    try:
        # Test connection
        client.ping()
        print("Successfully connected to InfluxDB")
        
        if args.check_data:
            check_data_availability(client, args.device)
            return
        
        # Get energy data
        energy_data = get_energy_data(client, start_time, end_time, args.device)
        
        if not energy_data:
            print("No energy data found for the specified time range")
            return
        
        # Create XML file
        create_espi_xml(energy_data, start_time, end_time, args.output)
        print(f"Successfully created ESPI XML file: {args.output}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == '__main__':
    main() 