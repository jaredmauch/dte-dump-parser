#!/usr/bin/python3

from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import pytz
import time
import numpy as np
from scipy import stats

# pulls data from influxdb for iotawatt device
#

# InfluxDB connection parameters
INFLUX_HOST = 'localhost'
INFLUX_DB = 'house_power'
INFLUX_MEASUREMENT = 'voltage'

# Voltage threshold and minimum duration
VOLTAGE_THRESHOLD = 110
MIN_DURATION_SECONDS = 300  # DTE says outages less than 5 mins dont matter

def connect_to_influx():
    """Connect to InfluxDB and return the client"""
    return InfluxDBClient(host=INFLUX_HOST, database=INFLUX_DB)

def get_voltage_data(client):
    """Fetch all voltage data from InfluxDB"""
    query = f'SELECT * FROM {INFLUX_MEASUREMENT}'
    print(f"Sending query at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()
    result = client.query(query)
    end_time = time.time()
    print(f"Query completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Query execution time: {end_time - start_time:.2f} seconds")
    return list(result.get_points())

def find_violations(data):
    """Find periods where voltage was below threshold for more than minimum duration"""
    violations = []
    current_violation = None
    
    for point in data:
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
        value = float(point['value'])  # Ensure value is a float
        
        if value < VOLTAGE_THRESHOLD:
            if current_violation is None:
                current_violation = {
                    'start': timestamp,
                    'end': timestamp,
                    'values': [value]
                }
            else:
                current_violation['end'] = timestamp
                current_violation['values'].append(value)
        else:
            if current_violation is not None:
                duration = (current_violation['end'] - current_violation['start']).total_seconds()
                if duration >= MIN_DURATION_SECONDS:
                    violations.append(current_violation)
                current_violation = None
    
    # Check if there's a violation at the end of the data
    if current_violation is not None:
        duration = (current_violation['end'] - current_violation['start']).total_seconds()
        if duration >= MIN_DURATION_SECONDS:
            violations.append(current_violation)
    
    return violations

def format_timestamp(timestamp):
    """Format timestamp in YYYY-MM-DD HH:MM:SS format"""
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

def format_duration(minutes):
    """Format duration in days, hours, and minutes"""
    days = int(minutes // (24 * 60))
    remaining_minutes = minutes % (24 * 60)
    hours = int(remaining_minutes // 60)
    remaining_minutes = int(remaining_minutes % 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if remaining_minutes > 0 or not parts:
        parts.append(f"{remaining_minutes} minute{'s' if remaining_minutes != 1 else ''}")
    
    return " ".join(parts)

def analyze_trends(violations):
    """Analyze trends in violations and make predictions"""
    if not violations:
        return None, None, None, None
    
    # Calculate yearly statistics
    years = {}
    for violation in violations:
        year = violation['start'].year
        if year not in years:
            years[year] = {'count': 0, 'total_duration': 0}
        years[year]['count'] += 1
        years[year]['total_duration'] += (violation['end'] - violation['start']).total_seconds() / 3600  # in hours
    
    # Calculate availability
    total_hours = 24 * 365.25  # Average year length
    yearly_availability = {year: (1 - (data['total_duration'] / total_hours)) * 100 
                          for year, data in years.items()}
    
    # Calculate trends
    years_list = sorted(years.keys())
    counts = [years[year]['count'] for year in years_list]
    durations = [years[year]['total_duration'] for year in years_list]
    
    # Linear regression for counts
    count_slope, count_intercept, count_r_value, _, _ = stats.linregress(years_list, counts)
    count_trend = "increasing" if count_slope > 0 else "decreasing"
    
    # Linear regression for durations
    duration_slope, duration_intercept, duration_r_value, _, _ = stats.linregress(years_list, durations)
    duration_trend = "increasing" if duration_slope > 0 else "decreasing"
    
    # Predict next 10 years
    future_years = range(max(years_list) + 1, max(years_list) + 11)
    predicted_counts = [count_slope * year + count_intercept for year in future_years]
    predicted_durations = [duration_slope * year + duration_intercept for year in future_years]
    
    return yearly_availability, count_trend, duration_trend, (future_years, predicted_counts, predicted_durations)

def main():
    try:
        # Connect to InfluxDB
        client = connect_to_influx()
        
        # Get voltage data
        print("Fetching voltage data...")
        data = get_voltage_data(client)
        print(f"Retrieved {len(data)} data points")
        
        # Find violations
        print("Analyzing data for violations...")
        violations = find_violations(data)
        
        # Print results
        print("\nVoltage Violations (below 110V for more than 10 seconds):")
        print("=" * 80)
        for i, violation in enumerate(violations, 1):
            start_time = format_timestamp(violation['start'])
            end_time = format_timestamp(violation['end'])
            duration_minutes = (violation['end'] - violation['start']).total_seconds() / 60
            min_voltage = min(violation['values'])
            
            print(f"Violation #{i}:")
            print(f"  Start Time: {start_time}")
            print(f"  End Time:   {end_time}")
            print(f"  Duration:   {format_duration(duration_minutes)}")
            print(f"  Min Voltage: {min_voltage:.2f}V")
            print("-" * 80)
        
        print(f"\nTotal violations found: {len(violations)}")
        
        # Analyze trends and make predictions
        yearly_availability, count_trend, duration_trend, predictions = analyze_trends(violations)
        
        if yearly_availability:
            print("\nYearly Availability Analysis:")
            print("=" * 80)
            for year, availability in sorted(yearly_availability.items()):
                print(f"{year}: {availability:.4f}% availability")
            
            print("\nTrend Analysis:")
            print("=" * 80)
            print(f"Frequency of outages is {count_trend}")
            print(f"Duration of outages is {duration_trend}")
            
            print("\n10-Year Predictions:")
            print("=" * 80)
            future_years, predicted_counts, predicted_durations = predictions
            for year, count, duration in zip(future_years, predicted_counts, predicted_durations):
                print(f"{year}:")
                print(f"  Predicted number of outages: {count:.1f}")
                print(f"  Predicted total outage duration: {duration:.1f} hours")
                print(f"  Predicted availability: {(1 - (duration / (24 * 365.25))) * 100:.4f}%")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 
