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
from datetime import datetime, date
from typing import Dict, List, Tuple

# Local imports
from dte_parser_lib import (
    load_meter_data,
    MeterData,
    timestamp_to_date,
    timestamp_to_datetime,
    daylight_hours,
    calculate_hourly_statistics
)

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

def calculate_budget_statistics(
    meter_data: MeterData,
    budget_kwh: float,
    duration_hours: int
) -> Tuple[Dict[date, float], float, float, float, float, float, float, float, float]:
    """Calculate statistics for periods exceeding the kWh budget.
    
    Args:
        meter_data: MeterData object containing hourly readings
        budget_kwh: Maximum allowed kWh for the period
        duration_hours: Duration in hours to analyze
        
    Returns:
        Tuple containing:
        - Dictionary mapping dates to watt shortfalls
        - Minimum watt shortfall
        - 25th percentile watt shortfall
        - Average watt shortfall
        - Median watt shortfall
        - 75th percentile watt shortfall
        - 90th percentile watt shortfall
        - 95th percentile watt shortfall
        - Peak watt shortfall
    """
    # Find all periods exceeding the budget
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    # Calculate watt shortfalls for each period
    watt_shortfalls = []
    date_shortfalls = {}
    
    for start_ts, end_ts, total_kwh in exceeded_periods:
        # Calculate average watt shortfall
        shortfall_kw = (total_kwh - budget_kwh) / duration_hours
        watt_shortfalls.append(shortfall_kw * 1000)  # Convert to watts
        
        # Store shortfall by date
        date_str = timestamp_to_date(start_ts)
        date_shortfalls[datetime.strptime(date_str, '%Y-%m-%d').date()] = shortfall_kw * 1000
    
    if not watt_shortfalls:
        return {}, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    
    # Sort shortfalls for percentile calculations
    watt_shortfalls.sort()
    
    def percentile(p):
        """Calculate the p-th percentile using linear interpolation."""
        n = len(watt_shortfalls)
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 >= n:
            return watt_shortfalls[-1]
        return watt_shortfalls[f] * (1 - c) + watt_shortfalls[f + 1] * c
    
    # Calculate statistics
    min_shortfall = min(watt_shortfalls)
    p25_shortfall = percentile(0.25)
    avg_shortfall = sum(watt_shortfalls) / len(watt_shortfalls)
    median_shortfall = percentile(0.50)
    p75_shortfall = percentile(0.75)
    p90_shortfall = percentile(0.90)
    p95_shortfall = percentile(0.95)
    peak_shortfall = max(watt_shortfalls)
    
    return (
        date_shortfalls,
        min_shortfall,
        p25_shortfall,
        avg_shortfall,
        median_shortfall,
        p75_shortfall,
        p90_shortfall,
        p95_shortfall,
        peak_shortfall
    )

def print_budget_exceeded_periods(meter_data: MeterData, budget_kwh: float, duration_hours: int):
    """Print periods that exceed the specified kWh budget.
    
    Args:
        meter_data: MeterData object containing hourly readings
        budget_kwh: Maximum allowed kWh for the period
        duration_hours: Duration in hours to analyze
    """
    # Find periods exceeding budget
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    if not exceeded_periods:
        print(f"\nNo periods found exceeding {budget_kwh:.1f} kWh over {duration_hours} hours")
        return
    
    print(f"\nPeriods exceeding {budget_kwh:.1f} kWh over {duration_hours} hours:")
    print("-" * 80)
    print(f"{'Date':<12} {'Excess kWh':<12} {'Daylight Hours':<15} {'Watt Shortfall':<15}")
    print("-" * 80)
    
    # Calculate statistics
    date_shortfalls, min_shortfall, p25_shortfall, avg_shortfall, median_shortfall, \
    p75_shortfall, p90_shortfall, p95_shortfall, peak_shortfall = calculate_budget_statistics(
        meter_data, budget_kwh, duration_hours
    )
    
    # Print each exceeded period
    for start_ts, end_ts, total_kwh in exceeded_periods:
        date_str = timestamp_to_date(start_ts)
        excess_kwh = total_kwh - budget_kwh
        daylight = daylight_hours(datetime.fromtimestamp(start_ts).date())
        shortfall = date_shortfalls.get(datetime.strptime(date_str, '%Y-%m-%d').date(), 0.0)
        
        print(
            f"{date_str:<12} {excess_kwh:>11.1f} {daylight:>14.1f} {shortfall:>14.1f}"
        )
    
    # Print summary statistics
    print("\nBudget Analysis Summary:")
    print("-" * 80)
    
    # Calculate total days and days exceeding budget
    total_days = len(meter_data.daily_totals)
    days_exceeding = len(date_shortfalls)
    days_within = total_days - days_exceeding
    
    print(f"Total days in source data: {total_days}")
    print(f"Days within {budget_kwh:.1f} kWh budget: {days_within} ({days_within/total_days*100:.1f}%)")
    print(f"Days exceeding {budget_kwh:.1f} kWh budget: {days_exceeding} ({days_exceeding/total_days*100:.1f}%)")
    
    print("\nWatt Shortfall Statistics:")
    print(f"Minimum: {min_shortfall:.1f} W")
    print(f"25th percentile: {p25_shortfall:.1f} W")
    print(f"Average: {avg_shortfall:.1f} W")
    print(f"Median: {median_shortfall:.1f} W")
    print(f"75th percentile: {p75_shortfall:.1f} W")
    print(f"90th percentile: {p90_shortfall:.1f} W")
    print(f"95th percentile: {p95_shortfall:.1f} W")
    print(f"Peak: {peak_shortfall:.1f} W")

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

def print_hourly_summary(meter_data: MeterData) -> None:
    """Print a detailed hourly usage summary.
    
    Args:
        meter_data: MeterData object containing hourly readings
    """
    hourly_stats = calculate_hourly_statistics(meter_data)
    
    print("\nHourly Usage Summary:")
    print("-" * 80)
    print(f"{'Hour':<6} {'Min':>8} {'25th%':>8} {'Median':>8} {'75th%':>8} {'90th%':>8} {'Avg':>8} {'Max':>8}")
    print("-" * 80)
    
    for hour in range(24):
        stats = hourly_stats[hour]
        print(
            f"{hour:02d}:00 {stats[0]:>8.2f} {stats[1]:>8.2f} {stats[2]:>8.2f} "
            f"{stats[3]:>8.2f} {stats[4]:>8.2f} {stats[5]:>8.2f} {stats[6]:>8.2f}"
        )

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Parse and analyze DTE electric usage data.')
    parser.add_argument(
        'xml_file',
        help='XML file containing electric usage data'
    )
    parser.add_argument(
        '--battery-size-kwh',
        type=float,
        default=30.0,
        help='Battery capacity in kWh (default: 30.0)'
    )
    parser.add_argument(
        '--battery-runtime-hours',
        type=int,
        default=24,
        help='Battery runtime period in hours (default: 24)'
    )
    parser.add_argument(
        '--hourly-summary',
        action='store_true',
        help='Print detailed hourly usage summary'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed processing information'
    )
    args = parser.parse_args()
    
    # Load meter data
    meter_data = load_meter_data([args.xml_file], args.verbose)
    
    # Print report for each meter
    for meter_id, data in meter_data.items():
        print_meter_report(data)
        
        if args.hourly_summary:
            print_hourly_summary(data)
        
        print_budget_exceeded_periods(data, args.battery_size_kwh, args.battery_runtime_hours)

if __name__ == '__main__':
    main() 