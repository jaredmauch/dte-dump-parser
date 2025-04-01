#!/usr/bin/env python3

"""
DTE Electric Usage XML Parser Library

This library provides functionality for parsing DTE electric usage data from XML files
and analyzing power consumption patterns. It supports:
- Parsing hourly and daily usage data
- Calculating peak and average usage
- Analyzing periods exceeding specified kWh budgets
- Computing daylight hours for solar analysis
"""

# Standard library imports
from collections import defaultdict
from datetime import datetime, date
from typing import Dict, List, Tuple

# Third-party imports
import xml.etree.ElementTree as ET
from astral import LocationInfo
from astral.sun import sun

def daylight_hours(target_date: date = None) -> float:
    """Calculate the number of daylight hours for a given date in Ann Arbor."""
    if target_date is None:
        target_date = date.today()
    
    # For future dates, use the current year
    current_year = date.today().year
    if target_date.year > current_year:
        target_date = date(current_year, target_date.month, target_date.day)
    
    # Create location object for Ann Arbor
    location = LocationInfo(
        name="Ann Arbor",
        region="USA",
        timezone="America/Detroit",
        latitude=42.2808,
        longitude=-83.7430
    )
    
    try:
        s = sun(location.observer, date=target_date)
        return abs((s['sunset'] - s['sunrise']).total_seconds() / 3600.0)
    except ValueError:
        # Return reasonable defaults based on month
        month = target_date.month
        if month in [12, 1, 2]:
            return 9.0
        if month in [3, 4, 5]:
            return 12.0
        if month in [6, 7, 8]:
            return 15.0
        return 12.0  # Fall months

class MeterData:
    """Class to store and manage electric meter data.
    
    This class holds all the data for a single electric meter, including:
    - Hourly readings (timestamp -> kW)
    - Daily totals (date -> kWh)
    - File coverage information
    """
    def __init__(self, title: str, meter_id: str):
        self.title = title
        self.meter_id = meter_id
        self.hourly_readings: Dict[int, float] = {}  # timestamp -> value in kW
        self.daily_totals: Dict[str, float] = defaultdict(float)  # date -> total kWh
        self.file_coverage: Dict[str, Tuple[int, int]] = {}  # file -> (first_ts, last_ts)

def timestamp_to_date(timestamp: int) -> str:
    """Convert Unix timestamp to YYYY-MM-DD format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

def timestamp_to_datetime(timestamp: int) -> str:
    """Convert Unix timestamp to YYYY-MM-DD HH:MM:SS format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def watts_to_kilowatts(watts: float) -> float:
    """Convert watts to kilowatts."""
    return watts / 1000.0

def process_interval_reading(
    reading: ET.Element,
    meter_data: MeterData,
    ns: Dict[str, str],
    verbose: bool = False
) -> Tuple[int, int]:
    """Process a single interval reading and update meter data.
    
    This function extracts timing and value information from an interval reading
    element and updates the meter data if it's an hourly reading.
    
    Args:
        reading: The XML element containing the interval reading
        meter_data: The MeterData object to update
        ns: XML namespace dictionary
        verbose: Whether to print detailed processing information
        
    Returns:
        Tuple of (start_time, duration) in seconds
    """
    time_period = reading.find('espi:timePeriod', ns)
    if time_period is None:
        return (0, 0)
        
    start_elem = time_period.find('espi:start', ns)
    duration_elem = time_period.find('espi:duration', ns)
    value_elem = reading.find('espi:value', ns)
    
    if not all(elem is not None for elem in [start_elem, duration_elem, value_elem]):
        return (0, 0)
        
    start_time = int(start_elem.text)
    duration = int(duration_elem.text)
    value = int(value_elem.text)
    
    # Only process hourly readings (3600 seconds)
    if duration == 3600:
        value_kw = watts_to_kilowatts(value)
        date_str = timestamp_to_date(start_time)
        meter_data.hourly_readings[start_time] = value_kw
        meter_data.daily_totals[date_str] += value_kw
        
        if verbose:
            print(f"Reading: {timestamp_to_datetime(start_time)} = {value_kw:.2f} kW")
    
    return (start_time, duration)

def process_interval_block(
    block: ET.Element,
    meter_data: MeterData,
    ns: Dict[str, str],
    verbose: bool = False
) -> List[int]:
    """Process a single interval block of readings.
    
    This function processes an interval block element, which contains multiple
    interval readings. It extracts timing information and processes each reading.
    
    Args:
        block: The XML element containing the interval block
        meter_data: The MeterData object to update
        ns: XML namespace dictionary
        verbose: Whether to print detailed processing information
        
    Returns:
        List of timestamps for processed readings
    """
    timestamps = []
    
    if verbose:
        interval = block.find('espi:interval', ns)
        if interval is not None:
            block_start = int(interval.find('espi:start', ns).text)
            block_dur = int(interval.find('espi:duration', ns).text)
            print("\nInterval Block:")
            print(f"Duration: {block_dur} seconds ({block_dur/3600:.1f} hours)")
            print(f"Start: {timestamp_to_datetime(block_start)}")
    
    for reading in block.findall('espi:IntervalReading', ns):
        start_time, _ = process_interval_reading(reading, meter_data, ns, verbose)
        if start_time > 0:
            timestamps.append(start_time)
    
    return timestamps

def parse_xml_file(file_path: str, all_meter_data: Dict[str, MeterData], verbose: bool = False) -> None:
    """Parse an XML file and update the meter data dictionary.
    
    This function processes an ESPI XML file containing electric usage data.
    It extracts hourly readings, calculates daily totals, and tracks file coverage.
    
    Args:
        file_path: Path to the XML file to parse
        all_meter_data: Dictionary of MeterData objects to update
        verbose: Whether to print detailed processing information
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    # Define namespaces used in ESPI XML format
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'espi': 'http://naesb.org/espi'
    }
    
    # Track file timestamps for coverage info
    file_timestamps = []
    first_timestamp = None
    last_timestamp = None
    hourly_readings_count = 0
    
    # Find all entries with usage data
    for entry in root.findall('.//atom:entry', ns):
        # Get meter ID from the entry
        meter_id = None
        for link in entry.findall('atom:link', ns):
            if link.get('rel') == 'self' and '/UsagePoint/' in link.get('href', ''):
                meter_id = link.get('href').split('/UsagePoint/')[-1].split('/')[0]
                break
        
        if meter_id and meter_id in all_meter_data:
            # Find all interval blocks in the content
            content = entry.find('atom:content', ns)
            if content is not None:
                for block in content.findall('.//espi:IntervalBlock', ns):
                    timestamps = process_interval_block(
                        block,
                        all_meter_data[meter_id],
                        ns,
                        verbose
                    )
                    file_timestamps.extend(timestamps)
                    hourly_readings_count += len(timestamps)
                    
                    # Update first/last timestamps
                    if timestamps:
                        block_min = min(timestamps)
                        block_max = max(timestamps)
                        if first_timestamp is None or block_min < first_timestamp:
                            first_timestamp = block_min
                        if last_timestamp is None or block_max > last_timestamp:
                            last_timestamp = block_max
    
    # Print file timestamp range and update meter coverage
    if first_timestamp is not None and last_timestamp is not None:
        print("\nXML File Date Range:")
        print(f"Start: {timestamp_to_datetime(first_timestamp)}")
        print(f"End:   {timestamp_to_datetime(last_timestamp)}")
        days_covered = (last_timestamp - first_timestamp) / (24 * 3600)
        print(f"Total Period: {days_covered:.1f} days")
        print(f"Total Hourly Readings: {hourly_readings_count}")
        
        # Update meter coverage
        for meter_id in all_meter_data:
            all_meter_data[meter_id].file_coverage[file_path] = (
                first_timestamp,
                last_timestamp
            )

def load_meter_data(file_paths: List[str], verbose: bool = False) -> Dict[str, MeterData]:
    """Load meter data from multiple XML files.
    
    This function processes multiple XML files and returns a dictionary of MeterData objects.
    
    Args:
        file_paths: List of paths to XML files to process
        verbose: Whether to print detailed processing information
        
    Returns:
        Dictionary mapping meter IDs to MeterData objects
    """
    # First pass: Find all meters
    all_meter_data = {}
    
    for file_path in file_paths:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Define namespaces
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'espi': 'http://naesb.org/espi'
        }
        
        # Find all UsagePoint entries
        for entry in root.findall('.//atom:entry', ns):
            for link in entry.findall('atom:link', ns):
                if link.get('rel') == 'self' and '/UsagePoint/' in link.get('href', ''):
                    meter_id = link.get('href').split('/UsagePoint/')[-1].split('/')[0]
                    title = entry.find('atom:title', ns).text
                    if meter_id not in all_meter_data:
                        all_meter_data[meter_id] = MeterData(title, meter_id)
    
    # Second pass: Process readings
    print("Processing XML files...")
    for file_path in file_paths:
        parse_xml_file(file_path, all_meter_data, verbose)
    
    return all_meter_data

def calculate_hourly_statistics(meter_data: MeterData) -> Dict[int, Tuple[float, float, float, float, float, float, float]]:
    """Calculate statistics for each hour of the day.
    
    This function analyzes the meter data to compute statistics for each hour,
    including min, max, average, and percentiles of usage across all days.
    
    Args:
        meter_data: MeterData object containing hourly readings
        
    Returns:
        Dictionary mapping hour (0-23) to tuple of (min, 25th percentile, median, 75th percentile, 90th percentile, average, max)
    """
    # Group readings by hour
    hourly_readings: Dict[int, List[float]] = {hour: [] for hour in range(24)}
    
    for timestamp, value in meter_data.hourly_readings.items():
        hour = datetime.fromtimestamp(timestamp).hour
        hourly_readings[hour].append(value)
    
    # Calculate statistics for each hour
    hourly_stats = {}
    
    def percentile(values: List[float], p: float) -> float:
        """Calculate the p-th percentile using linear interpolation."""
        if not values:
            return 0.0
        values.sort()
        n = len(values)
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 >= n:
            return values[-1]
        return values[f] * (1 - c) + values[f + 1] * c
    
    for hour in range(24):
        readings = hourly_readings[hour]
        if readings:
            hourly_stats[hour] = (
                min(readings),
                percentile(readings, 0.25),
                percentile(readings, 0.50),
                percentile(readings, 0.75),
                percentile(readings, 0.90),
                sum(readings) / len(readings),
                max(readings)
            )
        else:
            hourly_stats[hour] = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    
    return hourly_stats 