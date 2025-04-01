#!/usr/bin/env python3

"""
DTE Electric Usage Pattern Analyzer

This script analyzes electric usage patterns from DTE XML data files, identifying:
- Daily usage trends and significant changes
- Time-of-day patterns
- Seasonal variations
- Usage anomalies

The script uses the ESPI (Energy Service Provider Interface) XML format and builds
on the shared DTE parser library for data loading and processing.
"""

# Standard library imports
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple
import statistics

# Local imports
from dte_parser_lib import (
    load_meter_data,
    MeterData,
    timestamp_to_date,
    timestamp_to_datetime
)

def analyze_daily_changes(
    meter_data: MeterData,
    window_days: int = 7,
    threshold_percent: float = 20.0
) -> List[Tuple[date, float, float, float]]:
    """Analyze significant changes in daily usage patterns.
    
    This function identifies periods where usage changes significantly compared
    to the previous window of days.
    
    Args:
        meter_data: MeterData object containing usage data
        window_days: Number of days to use for moving average
        threshold_percent: Percentage change threshold for significance
        
    Returns:
        List of (date, previous_avg, current_avg, percent_change) tuples
    """
    significant_changes = []
    dates = sorted(meter_data.daily_totals.keys())
    
    if len(dates) < window_days * 2:
        return significant_changes
    
    for i in range(window_days, len(dates)):
        # Calculate averages for previous and current windows
        prev_window = [
            meter_data.daily_totals[dates[j]]
            for j in range(i - window_days, i)
        ]
        curr_window = [
            meter_data.daily_totals[dates[j]]
            for j in range(i, min(i + window_days, len(dates)))
        ]
        
        prev_avg = statistics.mean(prev_window)
        curr_avg = statistics.mean(curr_window)
        
        # Calculate percent change
        percent_change = ((curr_avg - prev_avg) / prev_avg) * 100
        
        # Record significant changes
        if abs(percent_change) >= threshold_percent:
            significant_changes.append((
                datetime.strptime(dates[i], '%Y-%m-%d').date(),
                prev_avg,
                curr_avg,
                percent_change
            ))
    
    return significant_changes

def analyze_hourly_patterns(
    meter_data: MeterData,
    min_days: int = 30
) -> Dict[int, Tuple[float, float, float, float]]:
    """Analyze hourly usage patterns.
    
    This function calculates statistics for each hour of the day to identify
    consistent patterns in usage.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
        
    Returns:
        Dictionary mapping hours to (avg, std_dev, min, max) tuples
    """
    # Group readings by hour
    hourly_groups: Dict[int, List[float]] = {hour: [] for hour in range(24)}
    
    for timestamp, value in meter_data.hourly_readings.items():
        hour = datetime.fromtimestamp(timestamp).hour
        hourly_groups[hour].append(value)
    
    # Calculate statistics for each hour
    hourly_stats = {}
    
    for hour, values in hourly_groups.items():
        if len(values) >= min_days:
            hourly_stats[hour] = (
                statistics.mean(values),
                statistics.stdev(values),
                min(values),
                max(values)
            )
        else:
            hourly_stats[hour] = (0.0, 0.0, 0.0, 0.0)
    
    return hourly_stats

def analyze_seasonal_patterns(
    meter_data: MeterData,
    window_days: int = 30
) -> List[Tuple[date, float, float]]:
    """Analyze seasonal patterns in usage.
    
    This function calculates moving averages to identify seasonal trends
    and variations in usage patterns.
    
    Args:
        meter_data: MeterData object containing usage data
        window_days: Window size for moving average
        
    Returns:
        List of (date, moving_avg, std_dev) tuples
    """
    seasonal_patterns = []
    dates = sorted(meter_data.daily_totals.keys())
    
    if len(dates) < window_days:
        return seasonal_patterns
    
    for i in range(len(dates) - window_days + 1):
        window_values = [
            meter_data.daily_totals[dates[j]]
            for j in range(i, i + window_days)
        ]
        
        moving_avg = statistics.mean(window_values)
        std_dev = statistics.stdev(window_values)
        
        seasonal_patterns.append((
            datetime.strptime(dates[i], '%Y-%m-%d').date(),
            moving_avg,
            std_dev
        ))
    
    return seasonal_patterns

def analyze_usage_periods(
    meter_data: MeterData,
    min_days: int = 5,
    threshold_percent: float = 15.0
) -> List[Tuple[date, date, float, float, float]]:
    """Analyze sustained periods of different usage levels.
    
    This function identifies periods where usage maintains a significantly
    different level compared to the previous period. It helps identify
    longer-term shifts in power consumption patterns.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required to consider a period
        threshold_percent: Percentage difference threshold for new period
        
    Returns:
        List of (start_date, end_date, avg_usage, prev_avg, percent_diff) tuples
    """
    dates = sorted(meter_data.daily_totals.keys())
    if len(dates) < min_days * 2:
        return []
    
    usage_periods = []
    period_start = datetime.strptime(dates[0], '%Y-%m-%d').date()
    period_values = [meter_data.daily_totals[dates[0]]]
    prev_period_avg = None
    
    for i in range(1, len(dates)):
        current_date = datetime.strptime(dates[i], '%Y-%m-%d').date()
        current_value = meter_data.daily_totals[dates[i]]
        current_avg = statistics.mean(period_values)
        
        # Check if this value represents a significant change
        if len(period_values) >= min_days:
            if abs(current_value - current_avg) > (threshold_percent / 100.0) * current_avg:
                # This could be the start of a new period
                next_values = []
                for j in range(i, min(i + min_days, len(dates))):
                    next_values.append(meter_data.daily_totals[dates[j]])
                
                if len(next_values) >= min_days:
                    next_avg = statistics.mean(next_values)
                    if abs(next_avg - current_avg) > (threshold_percent / 100.0) * current_avg:
                        # Confirmed new period
                        if len(period_values) >= min_days:
                            period_end = datetime.strptime(dates[i-1], '%Y-%m-%d').date()
                            period_avg = statistics.mean(period_values)
                            pct_diff = ((period_avg - prev_period_avg) / prev_period_avg * 100) if prev_period_avg else 0.0
                            
                            usage_periods.append((
                                period_start,
                                period_end,
                                period_avg,
                                prev_period_avg or period_avg,
                                pct_diff
                            ))
                            
                            # Start new period
                            period_start = current_date
                            period_values = [current_value]
                            prev_period_avg = period_avg
                            continue
        
        period_values.append(current_value)
    
    # Add final period if it meets minimum length
    if len(period_values) >= min_days:
        period_end = datetime.strptime(dates[-1], '%Y-%m-%d').date()
        period_avg = statistics.mean(period_values)
        pct_diff = ((period_avg - prev_period_avg) / prev_period_avg * 100) if prev_period_avg else 0.0
        
        usage_periods.append((
            period_start,
            period_end,
            period_avg,
            prev_period_avg or period_avg,
            pct_diff
        ))
    
    return usage_periods

def analyze_day_of_week_patterns(
    meter_data: MeterData,
    min_days: int = 30
) -> Dict[int, Tuple[float, float, float, float]]:
    """Analyze usage patterns by day of week.
    
    This function calculates statistics for each day of the week to identify
    consistent weekly patterns in usage.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
        
    Returns:
        Dictionary mapping day of week (0=Monday) to (avg, std_dev, min, max) tuples
    """
    # Group readings by day of week
    dow_groups: Dict[int, List[float]] = {dow: [] for dow in range(7)}
    
    for date_str, value in meter_data.daily_totals.items():
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        dow = date.weekday()  # 0 = Monday, 6 = Sunday
        dow_groups[dow].append(value)
    
    # Calculate statistics for each day of week
    dow_stats = {}
    
    for dow, values in dow_groups.items():
        if len(values) >= min_days / 7:  # Require at least 4 weeks of data
            dow_stats[dow] = (
                statistics.mean(values),
                statistics.stdev(values),
                min(values),
                max(values)
            )
        else:
            dow_stats[dow] = (0.0, 0.0, 0.0, 0.0)
    
    return dow_stats

def analyze_hourly_usage_periods(
    meter_data: MeterData,
    hour: int,
    min_days: int = 5,
    threshold_percent: float = 15.0
) -> List[Tuple[date, date, float, float, float]]:
    """Analyze sustained periods of different usage levels for a specific hour.
    
    This function identifies periods where usage during a specific hour maintains
    a significantly different level compared to the previous period.
    
    Args:
        meter_data: MeterData object containing usage data
        hour: Hour of day to analyze (0-23)
        min_days: Minimum number of days required to consider a period
        threshold_percent: Percentage difference threshold for new period
        
    Returns:
        List of (start_date, end_date, avg_usage, prev_avg, percent_diff) tuples
    """
    # Get all timestamps for the specified hour
    hour_timestamps = [
        ts for ts in meter_data.hourly_readings.keys()
        if datetime.fromtimestamp(ts).hour == hour
    ]
    
    if not hour_timestamps:
        return []
    
    # Sort timestamps and convert to dates
    hour_timestamps.sort()
    dates = [datetime.fromtimestamp(ts).date() for ts in hour_timestamps]
    
    if len(dates) < min_days * 2:
        return []
    
    usage_periods = []
    period_start = dates[0]
    period_values = [meter_data.hourly_readings[hour_timestamps[0]]]
    prev_period_avg = None
    
    for i in range(1, len(dates)):
        current_date = dates[i]
        current_value = meter_data.hourly_readings[hour_timestamps[i]]
        current_avg = statistics.mean(period_values)
        
        # Check if this value represents a significant change
        if len(period_values) >= min_days:
            if abs(current_value - current_avg) > (threshold_percent / 100.0) * current_avg:
                # This could be the start of a new period
                next_values = []
                for j in range(i, min(i + min_days, len(dates))):
                    next_values.append(meter_data.hourly_readings[hour_timestamps[j]])
                
                if len(next_values) >= min_days:
                    next_avg = statistics.mean(next_values)
                    if abs(next_avg - current_avg) > (threshold_percent / 100.0) * current_avg:
                        # Confirmed new period
                        if len(period_values) >= min_days:
                            period_end = dates[i-1]
                            period_avg = statistics.mean(period_values)
                            pct_diff = ((period_avg - prev_period_avg) / prev_period_avg * 100) if prev_period_avg else 0.0
                            
                            usage_periods.append((
                                period_start,
                                period_end,
                                period_avg,
                                prev_period_avg or period_avg,
                                pct_diff
                            ))
                            
                            # Start new period
                            period_start = current_date
                            period_values = [current_value]
                            prev_period_avg = period_avg
                            continue
        
        period_values.append(current_value)
    
    # Add final period if it meets minimum length
    if len(period_values) >= min_days:
        period_end = dates[-1]
        period_avg = statistics.mean(period_values)
        pct_diff = ((period_avg - prev_period_avg) / prev_period_avg * 100) if prev_period_avg else 0.0
        
        usage_periods.append((
            period_start,
            period_end,
            period_avg,
            prev_period_avg or period_avg,
            pct_diff
        ))
    
    return usage_periods

def print_analysis_report(
    meter_data: MeterData,
    window_days: int,
    threshold_percent: float
) -> None:
    """Print a comprehensive analysis report.
    
    Args:
        meter_data: MeterData object containing usage data
        window_days: Window size for trend analysis
        threshold_percent: Change threshold for significance
    """
    print("\nUsage Pattern Analysis Report")
    print("=" * 80)
    
    # Analyze usage periods
    print("\nSustained Usage Periods:")
    print("-" * 80)
    print(f"{'Period Start':<12} {'Period End':<12} {'Avg kWh':>10} {'Change %':>10} {'Days':>6}")
    print("-" * 80)
    
    periods = analyze_usage_periods(meter_data)
    for start_date, end_date, avg_usage, prev_avg, pct_diff in periods:
        days = (end_date - start_date).days + 1
        print(
            f"{start_date.strftime('%Y-%m-%d'):<12} "
            f"{end_date.strftime('%Y-%m-%d'):<12} "
            f"{avg_usage:>10.2f} "
            f"{pct_diff:>10.1f}% "
            f"{days:>6}"
        )
    
    # Analyze sustained periods for all hours
    print("\nSustained Periods by Hour:")
    print("-" * 80)
    print(f"{'Hour':<6} {'Period Start':<12} {'Period End':<12} {'Avg kW':>8} {'Change %':>10} {'Days':>6}")
    print("-" * 80)
    
    # Analyze all hours (0-23)
    for hour in range(24):
        periods = analyze_hourly_usage_periods(meter_data, hour)
        if periods:
            for start_date, end_date, avg_usage, prev_avg, pct_diff in periods:
                days = (end_date - start_date).days + 1
                print(
                    f"{hour:02d}:00 "
                    f"{start_date.strftime('%Y-%m-%d'):<12} "
                    f"{end_date.strftime('%Y-%m-%d'):<12} "
                    f"{avg_usage:>8.2f} "
                    f"{pct_diff:>10.1f}% "
                    f"{days:>6}"
                )
    
    # Analyze day of week patterns
    print("\nDay of Week Patterns:")
    print("-" * 80)
    print(f"{'Day':<10} {'Avg kWh':>10} {'Std Dev':>10} {'Min kWh':>10} {'Max kWh':>10}")
    print("-" * 80)
    
    dow_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_stats = analyze_day_of_week_patterns(meter_data)
    
    for dow in range(7):
        avg, std, min_val, max_val = dow_stats[dow]
        print(
            f"{dow_names[dow]:<10} "
            f"{avg:>10.2f} {std:>10.2f} "
            f"{min_val:>10.2f} {max_val:>10.2f}"
        )
    
    # Analyze significant daily changes
    print("\nSignificant Daily Changes:")
    print("-" * 80)
    print(f"{'Date':<12} {'Prev Avg':>10} {'Curr Avg':>10} {'Change %':>10}")
    print("-" * 80)
    
    changes = analyze_daily_changes(meter_data, window_days, threshold_percent)
    for date, prev_avg, curr_avg, pct_change in changes:
        print(
            f"{date.strftime('%Y-%m-%d'):<12} "
            f"{prev_avg:>10.2f} {curr_avg:>10.2f} {pct_change:>10.1f}%"
        )
    
    # Analyze hourly patterns
    print("\nHourly Usage Patterns:")
    print("-" * 80)
    print(f"{'Hour':<6} {'Avg kW':>8} {'Std Dev':>8} {'Min kW':>8} {'Max kW':>8}")
    print("-" * 80)
    
    hourly_stats = analyze_hourly_patterns(meter_data)
    for hour in range(24):
        avg, std, min_val, max_val = hourly_stats[hour]
        print(
            f"{hour:02d}:00 {avg:>8.2f} {std:>8.2f} "
            f"{min_val:>8.2f} {max_val:>8.2f}"
        )
    
    # Analyze seasonal patterns
    print("\nSeasonal Patterns:")
    print("-" * 80)
    print(f"{'Date':<12} {'30-day Avg':>12} {'Std Dev':>10}")
    print("-" * 80)
    
    patterns = analyze_seasonal_patterns(meter_data)
    for date, moving_avg, std_dev in patterns:
        print(
            f"{date.strftime('%Y-%m-%d'):<12} "
            f"{moving_avg:>12.2f} {std_dev:>10.2f}"
        )

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Analyze electric usage patterns from DTE XML data.'
    )
    parser.add_argument(
        'xml_file',
        help='XML file containing electric usage data'
    )
    parser.add_argument(
        '--window-days',
        type=int,
        default=7,
        help='Window size in days for trend analysis (default: 7)'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=20.0,
        help='Percentage change threshold for significance (default: 20.0)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed processing information'
    )
    args = parser.parse_args()
    
    # Load meter data
    meter_data = load_meter_data([args.xml_file], args.verbose)
    
    # Generate analysis report for each meter
    for meter_id, data in meter_data.items():
        print_analysis_report(data, args.window_days, args.threshold)

if __name__ == '__main__':
    main() 