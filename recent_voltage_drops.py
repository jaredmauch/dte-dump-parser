#!/usr/bin/python3

from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import pytz
import time
from collections import defaultdict
import sys

# InfluxDB connection parameters
INFLUX_HOST = 'influxdb'
INFLUX_DB = 'house_power'
INFLUX_MEASUREMENT = 'voltage'

# Voltage threshold
VOLTAGE_THRESHOLD = 110

def connect_to_influx():
    """Connect to InfluxDB and return the client"""
    return InfluxDBClient(host=INFLUX_HOST, database=INFLUX_DB)

def get_voltage_data(client):
    """Fetch voltage data from the past 366 days"""
    # Calculate timestamp for 366 days ago
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(days=366)
    
    # Format timestamps for InfluxDB query
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    query = f"SELECT * FROM {INFLUX_MEASUREMENT} WHERE value < {VOLTAGE_THRESHOLD} AND time >= '{start_str}' AND time <= '{end_str}' ORDER BY time DESC"
    print(f"Sending query at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Querying data from {start_str} to {end_str}")
    start_time = time.time()
    result = client.query(query)
    end_time = time.time()
    print(f"Query completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Query execution time: {end_time - start_time:.2f} seconds")
    return list(result.get_points())

def find_recent_voltage_drops(data):
    """Find all voltage drops below threshold from the data"""
    drops = []
    
    for point in data:
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
        value = float(point['value'])  # Ensure value is a float
        
        if value < VOLTAGE_THRESHOLD:
            drops.append({
                'timestamp': timestamp,
                'voltage': value
            })
    
    # Sort by timestamp (most recent first)
    drops.sort(key=lambda x: x['timestamp'], reverse=True)
    return drops

def group_drops_by_minute(drops):
    """Group voltage drops that occurred within the same minute or adjacent minutes"""
    if not drops:
        return []
    
    # First, group by minute
    grouped_drops = defaultdict(list)
    
    for drop in drops:
        # Round timestamp to the nearest minute (remove seconds and microseconds)
        minute_key = drop['timestamp'].replace(second=0, microsecond=0)
        grouped_drops[minute_key].append(drop)
    
    # Convert to list and sort by minute (most recent first)
    minute_groups = []
    for minute, minute_drops in grouped_drops.items():
        # Sort drops within the minute by timestamp (most recent first)
        minute_drops.sort(key=lambda x: x['timestamp'], reverse=True)
        
        minute_groups.append({
            'minute': minute,
            'drops': minute_drops,
            'count': len(minute_drops),
            'min_voltage': min(drop['voltage'] for drop in minute_drops),
            'max_voltage': max(drop['voltage'] for drop in minute_drops),
            'avg_voltage': sum(drop['voltage'] for drop in minute_drops) / len(minute_drops)
        })
    
    # Sort groups by minute (most recent first)
    minute_groups.sort(key=lambda x: x['minute'], reverse=True)
    
    # Now merge adjacent minute groups
    merged_groups = []
    i = 0
    
    while i < len(minute_groups):
        current_group = minute_groups[i].copy()
        merged_drops = current_group['drops'].copy()
        
        # Check if next groups are adjacent (within 1 minute)
        j = i + 1
        while j < len(minute_groups):
            time_diff = abs((current_group['minute'] - minute_groups[j]['minute']).total_seconds())
            
            # If adjacent (within 60 seconds), merge them
            if time_diff <= 60:
                merged_drops.extend(minute_groups[j]['drops'])
                current_group['minute'] = min(current_group['minute'], minute_groups[j]['minute'])  # Use earliest minute
                j += 1
            else:
                break
        
        # Update group with merged data
        current_group['drops'] = merged_drops
        current_group['count'] = len(merged_drops)
        current_group['min_voltage'] = min(drop['voltage'] for drop in merged_drops)
        current_group['max_voltage'] = max(drop['voltage'] for drop in merged_drops)
        current_group['avg_voltage'] = sum(drop['voltage'] for drop in merged_drops) / len(merged_drops)
        
        # Calculate duration of the outage
        if merged_drops:
            # Sort drops by timestamp to get start and end times
            sorted_drops = sorted(merged_drops, key=lambda x: x['timestamp'])
            start_time = sorted_drops[0]['timestamp']
            end_time = sorted_drops[-1]['timestamp']
            duration = end_time - start_time
            current_group['start_time'] = start_time
            current_group['end_time'] = end_time
            current_group['duration'] = duration
        
        merged_groups.append(current_group)
        i = j  # Skip the groups we just merged
    
    return merged_groups

def format_timestamp(timestamp):
    """Format timestamp in YYYY-MM-DD HH:MM:SS format"""
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

def format_duration_ago(timestamp):
    """Format how long ago the drop occurred"""
    now = datetime.now(pytz.UTC)
    duration = now - timestamp
    
    if duration.days > 0:
        return f"{duration.days:3d} day{'s' if duration.days != 1 else ''} ago"
    elif duration.seconds >= 3600:
        hours = duration.seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif duration.seconds >= 60:
        minutes = duration.seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    else:
        return f"{duration.seconds} second{'s' if duration.seconds != 1 else ''} ago"

def format_duration(duration):
    """Format duration in a human-readable format"""
    total_seconds = int(duration.total_seconds())
    
    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}m {seconds}s"
    else:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours}h {minutes}m {seconds}s"

def main():
    # Check for verbose flag
    verbose = '--verbose' in sys.argv
    
    try:
        # Connect to InfluxDB
        client = connect_to_influx()
        
        # Get voltage data
        print("Fetching voltage data...")
        data = get_voltage_data(client)
        print(f"Retrieved {len(data)} data points")
        
        # Find recent voltage drops
        print("Analyzing data for recent voltage drops...")
        recent_drops = find_recent_voltage_drops(data)
        
        # Group drops by minute
        print("Grouping drops by minute...")
        grouped_drops = group_drops_by_minute(recent_drops)
        
        # Print results
        print(f"\nVoltage Drop Groups (below {VOLTAGE_THRESHOLD}V):")
        print("=" * 80)
        
        if not grouped_drops:
            print("No voltage drops below threshold found in the data.")
        else:
            for i, group in enumerate(grouped_drops, 1):  # Show all groups
                minute_timestamp = format_timestamp(group['minute'])
                time_ago = format_duration_ago(group['minute'])
                
                if verbose:
                    # Display detailed information with duration, start, and end times
                    if 'duration' in group:
                        duration_str = format_duration(group['duration'])
                        start_str = format_timestamp(group['start_time'])
                        end_str = format_timestamp(group['end_time'])
                        print(f"Group #{i:3d}: {minute_timestamp} ({time_ago}) - Duration: {duration_str}")
                        print(f"           Start: {start_str} | End: {end_str}")
                        print(f"           Min: {group['min_voltage']:6.2f}V, Max: {group['max_voltage']:6.2f}V, Avg: {group['avg_voltage']:6.2f}V")
                    else:
                        print(f"Group #{i:3d}: {minute_timestamp} ({time_ago}) - Min: {group['min_voltage']:6.2f}V, Max: {group['max_voltage']:6.2f}V, Avg: {group['avg_voltage']:6.2f}V")
                    
                    # Add separator between groups
                    if i < len(grouped_drops):
                        print("-" * 80)
                else:
                    # Simple one-line format with duration inline
                    if 'duration' in group:
                        duration_str = format_duration(group['duration'])
                        print(f"Group #{i:3d}: {minute_timestamp} ({time_ago}) - Duration: {duration_str} - Min: {group['min_voltage']:6.2f}V, Max: {group['max_voltage']:6.2f}V, Avg: {group['avg_voltage']:6.2f}V")
                    else:
                        print(f"Group #{i:3d}: {minute_timestamp} ({time_ago}) - Min: {group['min_voltage']:6.2f}V, Max: {group['max_voltage']:6.2f}V, Avg: {group['avg_voltage']:6.2f}V")
        
        # Summary statistics
        if grouped_drops:
            total_drops = sum(group['count'] for group in grouped_drops)
            total_groups = len(grouped_drops)
            all_voltages = [drop['voltage'] for group in grouped_drops for drop in group['drops']]
            
            print(f"\nSummary Statistics:")
            print("=" * 80)
            print(f"Total bad voltage datapoints: {total_drops}")
            print(f"Total groups: {total_groups}")
#            print(f"Average drops per group: {total_drops / total_groups:.1f}")
            print(f"Average voltage during drops: {sum(all_voltages) / len(all_voltages):.2f}V")
#            print(f"Lowest voltage recorded: {min(all_voltages):.2f}V")
#            print(f"Highest voltage during drops: {max(all_voltages):.2f}V")
#            print(f"Voltage range: {max(all_voltages) - min(all_voltages):.2f}V")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 
