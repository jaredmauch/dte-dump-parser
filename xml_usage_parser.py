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

import xml.etree.ElementTree as ET
from datetime import datetime
import glob
import os
import sys
from typing import Dict, List, Tuple
from collections import defaultdict
import argparse
from astral import LocationInfo
from astral.sun import sun
from datetime import date

def daylight_hours(target_date: date = None, zip_code: str = "48158") -> float:
    """Calculate the number of daylight hours for a given date and location.
    
    This function uses the astral library to compute sunrise and sunset times
    for a specific location (default: Ann Arbor, MI) and calculates the total
    daylight hours. For future dates, it uses the current year to ensure valid
    calculations.
    
    Args:
        target_date: The date to calculate daylight hours for. Defaults to today.
        zip_code: The ZIP code for the location. Defaults to 48158 (Ann Arbor).
        
    Returns:
        Number of daylight hours as a float
    """
    if target_date is None:
        target_date = date.today()
    
    # For future dates, use the current year to calculate daylight hours
    current_year = date.today().year
    if target_date.year > current_year:
        target_date = date(current_year, target_date.month, target_date.day)
        
    # Create location object for Ann Arbor (48158)
    # Using approximate coordinates for Ann Arbor
    location = LocationInfo(
        name="Ann Arbor",
        region="USA",
        timezone="America/Detroit",
        latitude=42.2808,
        longitude=-83.7430
    )
    
    try:
        # Get sun information for the date
        s = sun(location.observer, date=target_date)
        
        # Calculate daylight hours (ensure positive value)
        daylight = abs((s['sunset'] - s['sunrise']).total_seconds() / 3600.0)
        
        return daylight
    except ValueError:
        # If we can't calculate daylight hours (e.g., for dates near the poles),
        # return a reasonable default value based on the month
        month = target_date.month
        if month in [12, 1, 2]:  # Winter months
            return 9.0
        elif month in [3, 4, 5]:  # Spring months
            return 12.0
        elif month in [6, 7, 8]:  # Summer months
            return 15.0
        else:  # Fall months
            return 12.0

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
        self.hourly_readings = {}  # timestamp -> value in kW
        self.daily_totals = defaultdict(float)  # date string -> total kWh
        self.file_coverage = {}  # file_path -> (first_timestamp, last_timestamp)

def timestamp_to_date(timestamp: int) -> str:
    """Convert Unix timestamp to YYYY-MM-DD format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

def timestamp_to_datetime(timestamp: int) -> str:
    """Convert Unix timestamp to YYYY-MM-DD HH:MM:SS format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def watts_to_kilowatts(watts: float) -> float:
    """Convert watts to kilowatts."""
    return watts / 1000.0

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
                interval_blocks = content.findall('.//espi:IntervalBlock', ns)
                
                for block in interval_blocks:
                    if verbose:
                        # Get block start time and duration
                        interval = block.find('espi:interval', ns)
                        if interval is not None:
                            block_start = int(interval.find('espi:start', ns).text)
                            block_dur = int(interval.find('espi:duration', ns).text)
                            print(f"\nInterval Block:")
                            print(f"  Duration: {block_dur} seconds ({block_dur/3600:.1f} hours)")
                            print(f"  Start: {timestamp_to_datetime(block_start)}")
                    
                    # Process each reading in the block
                    for reading in block.findall('espi:IntervalReading', ns):
                        time_period = reading.find('espi:timePeriod', ns)
                        if time_period is not None:
                            start_elem = time_period.find('espi:start', ns)
                            duration_elem = time_period.find('espi:duration', ns)
                            value_elem = reading.find('espi:value', ns)
                            
                            if start_elem is not None and duration_elem is not None and value_elem is not None:
                                start_time = int(start_elem.text)
                                duration = int(duration_elem.text)
                                value = int(value_elem.text)
                                
                                # Track first and last timestamps
                                if first_timestamp is None or start_time < first_timestamp:
                                    first_timestamp = start_time
                                if last_timestamp is None or start_time > last_timestamp:
                                    last_timestamp = start_time
                                
                                # Only process hourly readings (3600 seconds)
                                if duration == 3600 and meter_id in all_meter_data:
                                    hourly_readings_count += 1
                                    # Convert watts to kilowatts
                                    value_kw = watts_to_kilowatts(value)
                                    # Store hourly reading
                                    date_str = timestamp_to_date(start_time)
                                    all_meter_data[meter_id].hourly_readings[start_time] = value_kw
                                    # Add to daily total
                                    all_meter_data[meter_id].daily_totals[date_str] += value_kw
                                    # Track timestamp
                                    file_timestamps.append(start_time)
                                    
                                    if verbose:
                                        print(f"  Reading: {timestamp_to_datetime(start_time)} = {value_kw:.2f} kW")
    
    # Print file timestamp range and update meter coverage
    if first_timestamp is not None and last_timestamp is not None:
        print(f"\nXML File Date Range:")
        print(f"Start: {timestamp_to_datetime(first_timestamp)}")
        print(f"End:   {timestamp_to_datetime(last_timestamp)}")
        days_covered = (last_timestamp - first_timestamp) / (24 * 3600)
        print(f"Total Period: {days_covered:.1f} days")
        print(f"Total Hourly Readings: {hourly_readings_count}")
        
        # Update meter coverage
        for meter_id in all_meter_data:
            all_meter_data[meter_id].file_coverage[file_path] = (first_timestamp, last_timestamp)

def analyze_meter_data(meter_data: MeterData) -> Dict:
    """Analyze usage data and return peak values and statistics.
    
    This function calculates various statistics about the meter's usage,
    including peak values, averages, and date ranges.
    
    Args:
        meter_data: MeterData object to analyze
        
    Returns:
        Dictionary containing analysis results
    """
    hourly_values = list(meter_data.hourly_readings.values())
    daily_values = list(meter_data.daily_totals.values())
    
    # Get timestamps for date range
    timestamps = sorted(meter_data.hourly_readings.keys())
    date_range = "No data"
    if timestamps:
        min_timestamp = min(timestamps)
        max_timestamp = max(timestamps)
        start_date = datetime.fromtimestamp(min_timestamp).strftime('%Y-%m-%d')
        end_date = datetime.fromtimestamp(max_timestamp).strftime('%Y-%m-%d')
        
        # Calculate days based on timestamp difference
        days = (max_timestamp - min_timestamp) / (24 * 3600)
        
        # For same-day data, report as 1 day
        if start_date == end_date:
            date_range = f"{start_date} (1 day)"
        else:
            date_range = f"{start_date} to {end_date} ({days:.1f} days)"
    
    # Calculate unique days based on actual timestamps
    unique_days = len(set(timestamp_to_date(ts) for ts in timestamps))
    
    return {
        'peak_hourly': max(hourly_values) if hourly_values else 0,
        'peak_daily': max(daily_values) if daily_values else 0,
        'avg_hourly': sum(hourly_values) / len(hourly_values) if hourly_values else 0,
        'avg_daily': sum(daily_values) / len(daily_values) if daily_values else 0,
        'total_hours': len(hourly_values),
        'total_days': unique_days,
        'date_range': date_range,
        'file_coverage': meter_data.file_coverage
    }

def generate_report(all_meter_data: Dict[str, MeterData]) -> str:
    """Generate a formatted report string for all meters.
    
    This function creates a comprehensive report of electric usage data
    for all meters, including peak values, averages, and recent usage.
    
    Args:
        all_meter_data: Dictionary of MeterData objects to report on
        
    Returns:
        Formatted report string
    """
    report = ["Electric Usage Summary", "==================="]
    
    for meter_id, meter_data in sorted(all_meter_data.items()):
        analysis = analyze_meter_data(meter_data)
        meter_info = meter_data.meter_info
        
        # Calculate daily statistics
        daily_totals = sorted(meter_data.daily_totals.items())
        daily_stats = []
        if daily_totals:
            daily_stats.extend([
                "\nDaily Usage:",
                "------------"
            ])
            for date_str, total in daily_totals[-5:]:  # Show last 5 days
                daily_stats.append(f"{date_str}: {total:.2f} kWh")
        
        # Add file coverage information
        coverage_stats = [
            "\nData Coverage:",
            "-------------"
        ]
        for file_name, start_time, end_time in sorted(analysis['file_coverage']):
            coverage_stats.append(f"{file_name}:")
            coverage_stats.append(f"  {timestamp_to_datetime(start_time)} to {timestamp_to_datetime(end_time)}")
        
        report.extend([
            f"\nMeter: {meter_info['title']} ({meter_info['id']})",
            "-" * 50,
            f"Date Range:        {analysis['date_range']}",
            f"Peak Hourly Usage: {analysis['peak_hourly']:.2f} kW",
            f"Peak Daily Usage:  {analysis['peak_daily']:.2f} kWh",
            f"Average Hourly:    {analysis['avg_hourly']:.2f} kW",
            f"Average Daily:     {analysis['avg_daily']:.2f} kWh",
            f"Period Coverage:   {analysis['total_days']} days ({analysis['total_hours']} hours)"
        ] + daily_stats + coverage_stats)
    
    return "\n".join(report)

def print_meter_report(meter_data: MeterData):
    """Print a summary report for a meter.
    
    This function prints a detailed report of electric usage data
    for a single meter, including peak values, averages, and recent usage.
    
    Args:
        meter_data: MeterData object to report on
    """
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
    
    print(f"\nPeak Usage:")
    print(f"  Hourly: {peak_hourly:.2f} kW")
    print(f"  Daily:  {peak_daily:.2f} kWh")
    
    print(f"\nAverage Usage:")
    print(f"  Hourly: {avg_hourly:.2f} kW")
    print(f"  Daily:  {avg_daily:.2f} kWh")
    
    print(f"\nPeriod Coverage:")
    print(f"  {total_days:.0f} days ({len(meter_data.hourly_readings)} hours)")
    print(f"  From: {first_reading}")
    print(f"  To:   {last_reading}")
    
    print("\nFile Coverage:")
    for file_path, (start, end) in meter_data.file_coverage.items():
        print(f"  {os.path.basename(file_path)}:")
        print(f"    From: {timestamp_to_datetime(start)}")
        print(f"    To:   {timestamp_to_datetime(end)}")
    
    # Print recent daily usage
    print("\nRecent Daily Usage:")
    dates = sorted(meter_data.daily_totals.keys())
    for date in dates[-5:]:
        print(f"  {date}: {meter_data.daily_totals[date]:.2f} kWh")

def find_budget_exceeded_periods(meter_data: MeterData, budget_kwh: float, duration_hours: int) -> List[Tuple[int, int, float]]:
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
        total_kwh = sum(meter_data.hourly_readings[timestamps[j]] for j in range(i, i + duration_hours))
        
        # If total exceeds budget, record this period
        if total_kwh > budget_kwh:
            exceeded_periods.append((window_start, window_end, total_kwh))
    
    return exceeded_periods

def print_budget_exceeded_periods(meter_data: MeterData, budget_kwh: float, duration_hours: int):
    """Print periods that exceed the specified kWh budget.
    
    This function analyzes and prints information about periods where
    energy consumption exceeds the specified budget, including:
    - Individual periods with excess usage
    - Summary statistics about budget compliance
    - Peak watt shortfall during daylight hours
    
    Args:
        meter_data: MeterData object to analyze
        budget_kwh: Maximum allowed kWh for the period
        duration_hours: Duration in hours to analyze
    """
    # Calculate total days in source data first
    timestamps = sorted(meter_data.hourly_readings.keys())
    if not timestamps:
        return
        
    start_date = datetime.fromtimestamp(timestamps[0]).date()
    end_date = datetime.fromtimestamp(timestamps[-1]).date()
    total_days = (end_date - start_date).days + 1
    
    # Find exceeded periods
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    # Get unique dates where budget was exceeded
    exceeded_dates = set()
    for start_time, _, _ in exceeded_periods:
        exceeded_dates.add(datetime.fromtimestamp(start_time).date())
    exceeded_days = len(exceeded_dates)
    
    # Calculate percentage statistics
    within_budget_days = total_days - exceeded_days
    within_budget_percent = (within_budget_days / total_days) * 100
    exceeded_percent = (exceeded_days / total_days) * 100
    
    if not exceeded_periods:
        print(f"No periods found exceeding {budget_kwh:.1f} kWh over {duration_hours} hours\n")
        return
    
    # Group periods by date and find maximum excess for each day
    daily_max_excess = {}
    peak_watt_shortfall = 0
    for start_time, _, total_kwh in exceeded_periods:
        date = datetime.fromtimestamp(start_time).date()
        excess = total_kwh - budget_kwh
        if date not in daily_max_excess or excess > daily_max_excess[date]:
            daily_max_excess[date] = excess
            # Calculate watt shortfall for this excess
            daylight = daylight_hours(date)
            watt_shortfall = (excess / daylight) * 1000  # Convert to watts
            peak_watt_shortfall = max(peak_watt_shortfall, watt_shortfall)
    
    # Print individual periods that exceeded the budget
    print(f"Periods exceeding {budget_kwh:.1f} kWh over {duration_hours} hours:")
    for date, excess in sorted(daily_max_excess.items()):
        date_str = date.strftime('%Y-%m-%d')
        daylight = daylight_hours(date)
        watt_shortfall = (excess / daylight) * 1000  # Convert to watts
        print(f"{date_str} - Excess: {excess:.2f} kWh (Daylight: {daylight:.1f} hours, Watt Shortfall: {watt_shortfall:.1f} W)")
    
    # Print summary statistics at the end with clear separation
    print("\n" + "="*50)
    print("Budget Analysis Summary")
    print("="*50)
    print(f"Total days in source data: {total_days}")
    print(f"Days within {budget_kwh:.1f} kWh budget: {within_budget_days} ({within_budget_percent:.1f}%)")
    print(f"Days exceeding {budget_kwh:.1f} kWh budget: {exceeded_days} ({exceeded_percent:.1f}%)")
    print(f"Peak watt shortfall: {peak_watt_shortfall:.1f} W")
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
    parser.add_argument('--kwh-budget', type=float, help='Find periods exceeding specified kWh budget')
    parser.add_argument('--kwh-hours', type=int, default=24, help='Duration in hours for kWh budget analysis (default: 24)')
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