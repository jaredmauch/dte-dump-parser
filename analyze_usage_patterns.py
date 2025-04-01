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
from typing import Dict, List, Tuple, Optional
import statistics
import os
import shutil

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
) -> Dict[int, Tuple[float, float, float, float, float, str]]:
    """Analyze hourly usage patterns.
    
    This function calculates statistics for each hour of the day to identify
    consistent patterns in usage.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
        
    Returns:
        Dictionary mapping hours to (avg, std_dev, min, max, trend, trend_desc) tuples
        where trend is the slope and trend_desc is a human-readable description
    """
    # Group readings by hour
    hourly_groups: Dict[int, List[Tuple[datetime, float]]] = {hour: [] for hour in range(24)}
    
    for timestamp, value in meter_data.hourly_readings.items():
        dt = datetime.fromtimestamp(timestamp)
        hourly_groups[dt.hour].append((dt, value))
    
    # Calculate statistics for each hour
    hourly_stats = {}
    
    for hour, readings in hourly_groups.items():
        if len(readings) >= min_days:
            values = [r[1] for r in readings]
            dates = [r[0] for r in readings]
            
            # Calculate basic statistics
            avg = statistics.mean(values)
            std = statistics.stdev(values)
            min_val = min(values)
            max_val = max(values)
            
            # Calculate trend
            if len(dates) >= 7:  # Need at least a week of data for trend
                # Convert dates to numeric values (days since first date)
                first_date = dates[0]
                x_values = [(d - first_date).days for d in dates]
                
                # Calculate linear regression
                n = len(x_values)
                sum_x = sum(x_values)
                sum_y = sum(values)
                sum_xy = sum(x * y for x, y in zip(x_values, values))
                sum_xx = sum(x * x for x in x_values)
                
                if n * sum_xx - sum_x * sum_x != 0:
                    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
                    # Determine trend description
                    if abs(slope) < 0.01:  # Less than 0.01 kW/day change
                        trend_desc = "stable"
                    elif slope > 0:
                        trend_desc = "trending up"
                    else:
                        trend_desc = "trending down"
                else:
                    slope = 0
                    trend_desc = "stable"
            else:
                slope = 0
                trend_desc = "insufficient data"
            
            hourly_stats[hour] = (avg, std, min_val, max_val, slope, trend_desc)
        else:
            hourly_stats[hour] = (0.0, 0.0, 0.0, 0.0, 0.0, "insufficient data")
    
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

def get_terminal_width() -> int:
    """Get the current terminal width.
    
    Returns:
        Width of the terminal in characters
    """
    try:
        # Try to get terminal width using shutil
        width = shutil.get_terminal_size().columns
        # Ensure minimum width for readability
        return max(width, 80)
    except:
        # Fallback to default width if terminal size cannot be determined
        return 80

def create_usage_chart(
    meter_data: MeterData,
    width: Optional[int] = None,
    height: int = 20
) -> str:
    """Create an ASCII chart showing usage over time.
    
    Args:
        meter_data: MeterData object containing usage data
        width: Width of the chart in characters (default: terminal width)
        height: Height of the chart in characters
        
    Returns:
        String containing the ASCII chart
    """
    # Use terminal width if not specified
    if width is None:
        width = get_terminal_width()
    
    dates = sorted(meter_data.daily_totals.keys())
    if not dates:
        return "No data available for chart"
    
    # Get usage values and calculate min/max for scaling
    values = [meter_data.daily_totals[date] for date in dates]
    min_val = min(values)
    max_val = max(values)
    value_range = max_val - min_val
    
    if value_range == 0:
        return "No variation in usage data"
    
    # Define margins and chart area
    y_axis_width = 8  # Width for y-axis labels (6.1f + " |")
    chart_width = width - y_axis_width  # Available width for the chart area
    
    # Ensure minimum chart width
    if chart_width < 20:
        return f"Terminal width ({width}) is too narrow for chart display"
    
    # Calculate scaling factors
    x_scale = len(dates) / chart_width  # Scale based on available chart width
    y_scale = value_range / height
    
    # Create the chart
    chart_lines = []
    
    # Add title (truncate if needed)
    title = "Daily Usage Over Time"
    if len(title) > width:
        title = title[:width-3] + "..."
    chart_lines.append(title)
    chart_lines.append("=" * width)
    
    # Create the chart body (inverted)
    for y in range(1, height + 1):  # Changed range to go from bottom to top
        y_val = min_val + (y * y_scale)  # Changed calculation to start from min_val
        line = f"{y_val:6.1f} |"  # y-axis label
        
        # Add bars for each data point
        for i in range(len(dates)):
            if i % int(x_scale) == 0:  # Sample points based on x_scale
                value = values[i]
                if value <= y_val:  # Changed comparison to <= for inverted chart
                    line += " "  # Inverted: use space for bars
                else:
                    line += "█"  # Inverted: use block for background
        
        # Ensure line doesn't exceed width
        if len(line) > width:
            line = line[:width]
        else:
            line = line.ljust(width)
        chart_lines.append(line)
    
    # Add x-axis
    chart_lines.append("-" * width)
    
    # Add date labels at start and end (truncate if needed)
    start_date = dates[0]
    end_date = dates[-1]
    
    # Calculate available space for dates
    date_space = chart_width - 2  # Leave 1 space on each side
    date_format = "%Y-%m-%d"
    start_str = start_date
    end_str = end_date
    
    # Truncate dates if needed
    while len(start_str) + len(end_str) + 3 > date_space:  # 3 for spaces
        if len(start_str) > len(end_str):
            start_str = start_str[1:]
        else:
            end_str = end_str[:-1]
    
    date_line = " " * y_axis_width + f"{start_str} {' ' * (chart_width-len(start_str)-len(end_str)-1)} {end_str}"
    chart_lines.append(date_line)
    
    return "\n".join(chart_lines)

def project_usage_trend(
    meter_data: MeterData,
    weeks: int = 4
) -> Tuple[float, float, Optional[date]]:
    """Project future usage based on recent trends.
    
    Args:
        meter_data: MeterData object containing usage data
        weeks: Number of weeks to analyze for trend
        
    Returns:
        Tuple of (slope, intercept, zero_crossing_date)
    """
    dates = sorted(meter_data.daily_totals.keys())
    if len(dates) < weeks * 7:  # Need at least weeks worth of data
        return (0.0, 0.0, None)
    
    # Get recent data points
    recent_dates = dates[-weeks * 7:]
    recent_values = [meter_data.daily_totals[date] for date in recent_dates]
    
    # Convert dates to numeric values (days since first date)
    first_date = datetime.strptime(recent_dates[0], '%Y-%m-%d')
    x_values = [(datetime.strptime(date, '%Y-%m-%d') - first_date).days 
                for date in recent_dates]
    
    # Calculate linear regression
    n = len(x_values)
    sum_x = sum(x_values)
    sum_y = sum(recent_values)
    sum_xy = sum(x * y for x, y in zip(x_values, recent_values))
    sum_xx = sum(x * x for x in x_values)
    
    if n * sum_xx - sum_x * sum_x == 0:
        return (0.0, 0.0, None)
    
    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
    intercept = (sum_y - slope * sum_x) / n
    
    # Calculate zero crossing date if trend is decreasing
    zero_crossing_date = None
    if slope < 0:
        days_to_zero = -intercept / slope
        if days_to_zero > 0:
            zero_crossing_date = first_date + timedelta(days=int(days_to_zero))
    
    return (slope, intercept, zero_crossing_date)

def analyze_monthly_hourly_patterns(
    meter_data: MeterData,
    min_days: int = 5
) -> Dict[str, Dict[int, Tuple[float, float, float, float, float, str]]]:
    """Analyze hourly usage patterns for each month.
    
    This function calculates statistics for each hour of the day for each month
    to identify monthly variations in usage patterns.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
        
    Returns:
        Dictionary mapping month names to hourly statistics
    """
    # Group readings by month and hour
    monthly_hourly_groups: Dict[str, Dict[int, List[Tuple[datetime, float]]]] = {}
    
    for timestamp, value in meter_data.hourly_readings.items():
        dt = datetime.fromtimestamp(timestamp)
        month_key = dt.strftime('%Y-%m')
        
        if month_key not in monthly_hourly_groups:
            monthly_hourly_groups[month_key] = {hour: [] for hour in range(24)}
        
        monthly_hourly_groups[month_key][dt.hour].append((dt, value))
    
    # Calculate statistics for each month and hour
    monthly_stats = {}
    
    for month_key, hourly_groups in monthly_hourly_groups.items():
        month_stats = {}
        for hour, readings in hourly_groups.items():
            if len(readings) >= min_days:
                values = [r[1] for r in readings]
                dates = [r[0] for r in readings]
                
                # Calculate basic statistics
                avg = statistics.mean(values)
                std = statistics.stdev(values)
                min_val = min(values)
                max_val = max(values)
                
                # Calculate trend
                if len(dates) >= 7:  # Need at least a week of data for trend
                    # Convert dates to numeric values (days since first date)
                    first_date = dates[0]
                    x_values = [(d - first_date).days for d in dates]
                    
                    # Calculate linear regression
                    n = len(x_values)
                    sum_x = sum(x_values)
                    sum_y = sum(values)
                    sum_xy = sum(x * y for x, y in zip(x_values, values))
                    sum_xx = sum(x * x for x in x_values)
                    
                    if n * sum_xx - sum_x * sum_x != 0:
                        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
                        # Determine trend description
                        if abs(slope) < 0.01:  # Less than 0.01 kW/day change
                            trend_desc = "stable"
                        elif slope > 0:
                            trend_desc = "trending up"
                        else:
                            trend_desc = "trending down"
                    else:
                        slope = 0
                        trend_desc = "stable"
                else:
                    slope = 0
                    trend_desc = "insufficient data"
                
                month_stats[hour] = (avg, std, min_val, max_val, slope, trend_desc)
            else:
                month_stats[hour] = (0.0, 0.0, 0.0, 0.0, 0.0, "insufficient data")
        
        monthly_stats[month_key] = month_stats
    
    return monthly_stats

def print_monthly_hourly_patterns(
    meter_data: MeterData,
    min_days: int = 5
) -> None:
    """Print hourly usage patterns for each month.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
    """
    monthly_stats = analyze_monthly_hourly_patterns(meter_data, min_days)
    
    # Sort months chronologically
    sorted_months = sorted(monthly_stats.keys())
    
    for month_key in sorted_months:
        month_name = datetime.strptime(month_key, '%Y-%m').strftime('%B %Y')
        print(f"\nHourly Usage Patterns for {month_name}:")
        print("-" * 80)
        print(f"{'Hour':<6} {'Avg kW':>8} {'Std Dev':>8} {'Min kW':>8} {'Max kW':>8} {'Trend':<12}")
        print("-" * 80)
        
        month_stats = monthly_stats[month_key]
        for hour in range(24):
            avg, std, min_val, max_val, slope, trend_desc = month_stats[hour]
            print(
                f"{hour:02d}:00 {avg:>8.2f} {std:>8.2f} "
                f"{min_val:>8.2f} {max_val:>8.2f} {trend_desc:<12}"
            )

def analyze_daily_trends(
    meter_data: MeterData,
    min_days: int = 5
) -> Dict[int, Dict[int, Tuple[float, float, float, float, float, str]]]:
    """Analyze hourly usage patterns for each day of the week.
    
    This function calculates statistics for each hour of the day for each day of the week
    to identify daily variations in usage patterns.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
        
    Returns:
        Dictionary mapping days (0=Monday) to hourly statistics
    """
    # Group readings by day of week and hour
    daily_hourly_groups: Dict[int, Dict[int, List[Tuple[datetime, float]]]] = {
        day: {hour: [] for hour in range(24)} for day in range(7)
    }
    
    for timestamp, value in meter_data.hourly_readings.items():
        dt = datetime.fromtimestamp(timestamp)
        day_of_week = dt.weekday()
        daily_hourly_groups[day_of_week][dt.hour].append((dt, value))
    
    # Calculate statistics for each day and hour
    daily_stats = {}
    
    for day, hourly_groups in daily_hourly_groups.items():
        day_stats = {}
        for hour, readings in hourly_groups.items():
            if len(readings) >= min_days:
                values = [r[1] for r in readings]
                dates = [r[0] for r in readings]
                
                # Calculate basic statistics
                avg = statistics.mean(values)
                std = statistics.stdev(values)
                min_val = min(values)
                max_val = max(values)
                
                # Calculate trend
                if len(dates) >= 7:  # Need at least a week of data for trend
                    # Convert dates to numeric values (days since first date)
                    first_date = dates[0]
                    x_values = [(d - first_date).days for d in dates]
                    
                    # Calculate linear regression
                    n = len(x_values)
                    sum_x = sum(x_values)
                    sum_y = sum(values)
                    sum_xy = sum(x * y for x, y in zip(x_values, values))
                    sum_xx = sum(x * x for x in x_values)
                    
                    if n * sum_xx - sum_x * sum_x != 0:
                        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
                        # Determine trend description
                        if abs(slope) < 0.01:  # Less than 0.01 kW/day change
                            trend_desc = "stable"
                        elif slope > 0:
                            trend_desc = "trending up"
                        else:
                            trend_desc = "trending down"
                    else:
                        slope = 0
                        trend_desc = "stable"
                else:
                    slope = 0
                    trend_desc = "insufficient data"
                
                day_stats[hour] = (avg, std, min_val, max_val, slope, trend_desc)
            else:
                day_stats[hour] = (0.0, 0.0, 0.0, 0.0, 0.0, "insufficient data")
        
        daily_stats[day] = day_stats
    
    return daily_stats

def print_daily_trends(
    meter_data: MeterData,
    min_days: int = 5
) -> None:
    """Print hourly usage patterns for each day of the week.
    
    Args:
        meter_data: MeterData object containing usage data
        min_days: Minimum number of days required for analysis
    """
    daily_stats = analyze_daily_trends(meter_data, min_days)
    dow_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    for day in range(7):
        print(f"\nHourly Usage Patterns for {dow_names[day]}:")
        print("-" * 80)
        print(f"{'Hour':<6} {'Avg kW':>8} {'Std Dev':>8} {'Min kW':>8} {'Max kW':>8} {'Trend':<12}")
        print("-" * 80)
        
        day_stats = daily_stats[day]
        for hour in range(24):
            avg, std, min_val, max_val, slope, trend_desc = day_stats[hour]
            print(
                f"{hour:02d}:00 {avg:>8.2f} {std:>8.2f} "
                f"{min_val:>8.2f} {max_val:>8.2f} {trend_desc:<12}"
            )

def print_analysis_report(
    meter_data: MeterData,
    window_days: int,
    threshold_percent: float,
    show_seasonal: bool = False,
    show_monthly_trends: bool = False,
    show_daily_trends: bool = False
) -> None:
    """Print a comprehensive analysis report.
    
    Args:
        meter_data: MeterData object containing usage data
        window_days: Window size for trend analysis
        threshold_percent: Change threshold for significance
        show_seasonal: Whether to include seasonal pattern analysis
        show_monthly_trends: Whether to include monthly hourly patterns
        show_daily_trends: Whether to include daily hourly patterns
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
    print(f"{'Hour':<6} {'Avg kW':>8} {'Std Dev':>8} {'Min kW':>8} {'Max kW':>8} {'Trend':<12}")
    print("-" * 80)
    
    hourly_stats = analyze_hourly_patterns(meter_data)
    for hour in range(24):
        avg, std, min_val, max_val, slope, trend_desc = hourly_stats[hour]
        print(
            f"{hour:02d}:00 {avg:>8.2f} {std:>8.2f} "
            f"{min_val:>8.2f} {max_val:>8.2f} {trend_desc:<12}"
        )
    
    # Analyze seasonal patterns
    if show_seasonal:
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
    
    # Add usage chart
    print("\nUsage Chart:")
    print("-" * 80)
    print(create_usage_chart(meter_data))
    
    # Add trend projection
    print("\nUsage Trend Projection:")
    print("-" * 80)
    slope, intercept, zero_crossing_date = project_usage_trend(meter_data)
    
    if slope == 0:
        print("No significant trend detected in recent usage")
    else:
        trend_direction = "decreasing" if slope < 0 else "increasing"
        change_per_day = slope
        print(f"Current trend: {trend_direction} by {abs(change_per_day):.2f} kWh per day")
        
        # Project 30 days into the future
        last_date = datetime.strptime(sorted(meter_data.daily_totals.keys())[-1], '%Y-%m-%d')
        future_date = last_date + timedelta(days=30)
        days_forward = (future_date - last_date).days
        projected_usage = intercept + slope * days_forward
        
        print(f"Projected usage in 30 days: {projected_usage:.2f} kWh/day")
        
        if zero_crossing_date:
            print(f"Based on current trend, usage will reach zero by: {zero_crossing_date.strftime('%Y-%m-%d')}")
        else:
            print("Based on current trend, usage will not reach zero")

    # Add daily trends if requested
    if show_daily_trends:
        print_daily_trends(meter_data)

    # Add monthly hourly patterns if requested
    if show_monthly_trends:
        print_monthly_hourly_patterns(meter_data)

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
        '--show-seasonal',
        action='store_true',
        help='Include seasonal pattern analysis in the report'
    )
    parser.add_argument(
        '--monthly-trends',
        action='store_true',
        help='Include monthly hourly usage patterns in the report'
    )
    parser.add_argument(
        '--daily-trends',
        action='store_true',
        help='Include daily hourly usage patterns in the report'
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
        print_analysis_report(
            data,
            args.window_days,
            args.threshold,
            args.show_seasonal,
            args.monthly_trends,
            args.daily_trends
        )

if __name__ == '__main__':
    main() 