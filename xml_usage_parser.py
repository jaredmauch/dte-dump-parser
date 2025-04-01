#!/usr/bin/env python3

"""
DTE Electric Usage XML Parser

This script parses DTE electric usage data from XML files and provides analysis tools
for understanding power consumption patterns. It supports:
- Parsing hourly and daily usage data
- Calculating peak and average usage
- Analyzing periods exceeding specified kWh budgets
- Computing daylight hours for solar analysis
- Generating detailed usage reports

The script uses the ESPI (Energy Service Provider Interface) XML format,
which is a standard for energy usage data exchange.
"""

# Standard library imports
import argparse
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

def find_budget_exceeded_periods(
    meter_data: MeterData,
    budget_kwh: float,
    duration_hours: int
) -> List[Tuple[int, int, float]]:
    """Find periods that exceed the specified kWh budget over the given duration.
    
    This function analyzes the meter data to find periods where the total
    energy consumption exceeds the specified budget over the given duration.
    
    Args:
        meter_data: MeterData object containing hourly readings
        budget_kwh: Maximum allowed kWh for the period
        duration_hours: Duration in hours to analyze
        
    Returns:
        List of tuples (start_timestamp, end_timestamp, total_kwh) for periods exceeding budget
    """
    exceeded_periods = []
    timestamps = sorted(meter_data.hourly_readings.keys())
    
    if len(timestamps) < duration_hours:
        return exceeded_periods
    
    # Analyze each possible window of duration_hours
    for i in range(len(timestamps) - duration_hours + 1):
        window_start = timestamps[i]
        window_end = timestamps[i + duration_hours - 1]
        
        # Calculate total kWh for this window
        total_kwh = sum(
            meter_data.hourly_readings[timestamps[j]]
            for j in range(i, i + duration_hours)
        )
        
        # If total exceeds budget, record this period
        if total_kwh > budget_kwh:
            exceeded_periods.append((window_start, window_end, total_kwh))
    
    return exceeded_periods

def print_meter_report(meter_data: MeterData) -> None:
    """Print a summary report for a meter."""
    print(f"\nElectric Usage Summary for {meter_data.title}")
    print(f"Meter ID: {meter_data.meter_id}")
    
    if not meter_data.hourly_readings:
        print("No readings found for this meter.")
        return
        
    # Calculate statistics
    readings = list(meter_data.hourly_readings.values())
    peak_hourly = max(readings)
    avg_hourly = sum(readings) / len(readings)
    
    daily_values = list(meter_data.daily_totals.values())
    peak_daily = max(daily_values)
    avg_daily = sum(daily_values) / len(daily_values)
    
    # Get date range
    timestamps = sorted(meter_data.hourly_readings.keys())
    first_reading = timestamp_to_datetime(timestamps[0])
    last_reading = timestamp_to_datetime(timestamps[-1])
    total_days = (timestamps[-1] - timestamps[0]) / (24 * 3600) + 1
    
    print("\nPeak Usage:")
    print(f"Hourly: {peak_hourly:.2f} kW")
    print(f"Daily:  {peak_daily:.2f} kWh")
    
    print("\nAverage Usage:")
    print(f"Hourly: {avg_hourly:.2f} kW")
    print(f"Daily:  {avg_daily:.2f} kWh")
    
    print("\nPeriod Coverage:")
    print(f"{total_days:.0f} days ({len(meter_data.hourly_readings)} hours)")
    print(f"From: {first_reading}")
    print(f"To:   {last_reading}")
    
    print("\nFile Coverage:")
    for file_path, (start, end) in meter_data.file_coverage.items():
        print(f"{file_path}:")
        print(f"  From: {timestamp_to_datetime(start)}")
        print(f"  To:   {timestamp_to_datetime(end)}")
    
    # Print recent daily usage
    print("\nRecent Daily Usage:")
    dates = sorted(meter_data.daily_totals.keys())
    for date_str in dates[-5:]:
        print(f"{date_str}: {meter_data.daily_totals[date_str]:.2f} kWh")

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

def calculate_budget_statistics(
    meter_data: MeterData,
    budget_kwh: float,
    duration_hours: int
) -> Tuple[Dict[date, float], float, float, float, float, float, float, float, float]:
    """Calculate budget statistics for the meter data.
    
    Returns a tuple containing:
    - Dictionary mapping dates to their maximum excess kWh
    - Minimum watt shortfall
    - 25th percentile watt shortfall
    - Average watt shortfall
    - Median (50th percentile) watt shortfall
    - 75th percentile watt shortfall
    - 90th percentile watt shortfall
    - 95th percentile watt shortfall
    - Peak watt shortfall
    """
    # Find all periods that exceed the budget
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    # Track maximum excess for each date and collect watt shortfalls
    daily_max_excess = {}
    watt_shortfalls = []
    
    for start_time, _, total_kwh in exceeded_periods:
        reading_date = datetime.fromtimestamp(start_time).date()
        excess = total_kwh - budget_kwh
        
        # Update daily maximum if this is the highest excess for this date
        if reading_date not in daily_max_excess or excess > daily_max_excess[reading_date]:
            daily_max_excess[reading_date] = excess
            # Calculate watt shortfall based on daylight hours
            daylight = daylight_hours(reading_date)
            watt_shortfall = (excess / daylight) * 1000  # Convert to watts
            watt_shortfalls.append(watt_shortfall)
    
    # Return zeros if no periods exceeded the budget
    if not watt_shortfalls:
        return daily_max_excess, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    
    # Sort watt_shortfalls for percentile calculations
    watt_shortfalls.sort()
    n = len(watt_shortfalls)
    
    # Helper function to calculate percentiles using linear interpolation
    def percentile(p):
        """Calculate the p-th percentile using linear interpolation.
        
        Args:
            p: The percentile to calculate (0.0 to 1.0)
            
        Returns:
            The interpolated value at the specified percentile
        """
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 >= n:
            return watt_shortfalls[-1]
        return watt_shortfalls[f] * (1 - c) + watt_shortfalls[f + 1] * c
    
    # Calculate and return all statistics
    return (
        daily_max_excess,
        min(watt_shortfalls),
        percentile(0.25),  # 25th percentile
        sum(watt_shortfalls) / len(watt_shortfalls),  # average
        percentile(0.50),  # 50th percentile (median)
        percentile(0.75),  # 75th percentile
        percentile(0.90),  # 90th percentile
        percentile(0.95),  # 95th percentile
        max(watt_shortfalls)
    )

def print_budget_exceeded_periods(meter_data: MeterData, budget_kwh: float, duration_hours: int):
    """Print periods that exceed the specified kWh budget.
    
    This function analyzes the meter data to find periods exceeding the budget,
    calculates various statistics about the watt shortfalls, and prints a detailed report.
    
    Args:
        meter_data: The meter data to analyze
        budget_kwh: The kWh budget to check against
        duration_hours: The duration in hours to consider for each period
    """
    # Calculate total days in source data first
    timestamps = sorted(meter_data.hourly_readings.keys())
    if not timestamps:
        return
        
    start_date = datetime.fromtimestamp(timestamps[0]).date()
    end_date = datetime.fromtimestamp(timestamps[-1]).date()
    total_days = (end_date - start_date).days + 1
    
    # Find exceeded periods and calculate statistics
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    if not exceeded_periods:
        print(f"No periods found exceeding {budget_kwh:.1f} kWh over {duration_hours} hours\n")
        return
    
    # Get unique dates where budget was exceeded
    exceeded_dates = {
        datetime.fromtimestamp(start_time).date()
        for start_time, _, _ in exceeded_periods
    }
    exceeded_days = len(exceeded_dates)
    
    # Calculate percentage statistics
    within_budget_days = total_days - exceeded_days
    within_budget_percent = (within_budget_days / total_days) * 100
    exceeded_percent = (exceeded_days / total_days) * 100
    
    # Calculate daily maximums and watt shortfall statistics
    daily_max_excess, min_watt_shortfall, p25_watt_shortfall, avg_watt_shortfall, p50_watt_shortfall, p75_watt_shortfall, p90_watt_shortfall, p95_watt_shortfall, peak_watt_shortfall = calculate_budget_statistics(
        meter_data,
        budget_kwh,
        duration_hours
    )
    
    # Print individual periods that exceeded the budget
    print(f"Periods exceeding {budget_kwh:.1f} kWh over {duration_hours} hours:")
    for reading_date, excess in sorted(daily_max_excess.items()):
        date_str = reading_date.strftime('%Y-%m-%d')
        daylight = daylight_hours(reading_date)
        watt_shortfall = (excess / daylight) * 1000  # Convert to watts
        print(
            f"{date_str} - Excess: {excess:.2f} kWh "
            f"(Daylight: {daylight:.1f} hours, "
            f"Watt Shortfall: {watt_shortfall:.1f} W)"
        )
    
    # Print summary statistics
    print("\n" + "="*50)
    print("Budget Analysis Summary")
    print("="*50)
    print(f"Total days in source data: {total_days}")
    print(
        f"Days within {budget_kwh:.1f} kWh budget: "
        f"{within_budget_days} ({within_budget_percent:.1f}%)"
    )
    print(
        f"Days exceeding {budget_kwh:.1f} kWh budget: "
        f"{exceeded_days} ({exceeded_percent:.1f}%)"
    )
    print("\nWatt Shortfall Statistics:")
    print(f"Minimum:  {min_watt_shortfall:.1f} W")
    print(f"25th %:   {p25_watt_shortfall:.1f} W")
    print(f"Average:  {avg_watt_shortfall:.1f} W")
    print(f"Median:   {p50_watt_shortfall:.1f} W")
    print(f"75th %:   {p75_watt_shortfall:.1f} W")
    print(f"90th %:   {p90_watt_shortfall:.1f} W")
    print(f"95th %:   {p95_watt_shortfall:.1f} W")
    print(f"Peak:     {peak_watt_shortfall:.1f} W")
    print("="*50 + "\n")

def main():
    """Parse electric usage XML files and generate a summary report.
    
    This is the main entry point for the script. It:
    1. Parses command line arguments
    2. Identifies all meters in the XML file
    3. Processes the usage data
    4. Generates and prints reports
    5. Performs budget analysis if requested
    """
    parser = argparse.ArgumentParser(description='Parse electric usage XML files.')
    parser.add_argument('file_path', help='Path to the XML file')
    parser.add_argument('--verbose', action='store_true', help='Print detailed output')
    parser.add_argument(
        '--kwh-budget',
        type=float,
        help='Find periods exceeding specified kWh budget'
    )
    parser.add_argument(
        '--kwh-hours',
        type=int,
        default=24,
        help='Duration in hours for kWh budget analysis (default: 24)'
    )
    args = parser.parse_args()
    
    # First pass: Find all meters
    tree = ET.parse(args.file_path)
    root = tree.getroot()
    
    # Define namespaces
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'espi': 'http://naesb.org/espi'
    }
    
    all_meter_data = {}
    
    # Find all UsagePoint entries
    for entry in root.findall('.//atom:entry', ns):
        for link in entry.findall('atom:link', ns):
            if link.get('rel') == 'self' and '/UsagePoint/' in link.get('href', ''):
                meter_id = link.get('href').split('/UsagePoint/')[-1].split('/')[0]
                title = entry.find('atom:title', ns).text
                if meter_id not in all_meter_data:
                    all_meter_data[meter_id] = MeterData(title, meter_id)
    
    # Second pass: Process readings
    print("Processing XML file...")
    parse_xml_file(args.file_path, all_meter_data, args.verbose)
    
    # Generate report
    print("\nGenerating summary report...")
    for meter_data in all_meter_data.values():
        print_meter_report(meter_data)
        
        # Handle kWh budget analysis if requested
        if args.kwh_budget is not None:
            print_budget_exceeded_periods(meter_data, args.kwh_budget, args.kwh_hours)

if __name__ == '__main__':
    main() 