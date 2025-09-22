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

def print_budget_exceeded_periods(meter_data: MeterData, budget_kwh: float, duration_hours: int, verbose: bool = False):
    """Print periods that exceed the specified kWh budget.
    
    Args:
        meter_data: MeterData object containing hourly readings
        budget_kwh: Maximum allowed kWh for the period
        duration_hours: Duration in hours to analyze
        verbose: Whether to show detailed period table (default: False)
    """
    # Find periods exceeding budget
    exceeded_periods = find_budget_exceeded_periods(meter_data, budget_kwh, duration_hours)
    
    if not exceeded_periods:
        print(f"\nNo periods found exceeding {budget_kwh:.1f} kWh over {duration_hours} hours")
        return
    
    # Calculate statistics
    date_shortfalls, min_shortfall, p25_shortfall, avg_shortfall, median_shortfall, \
    p75_shortfall, p90_shortfall, p95_shortfall, peak_shortfall = calculate_budget_statistics(
        meter_data, budget_kwh, duration_hours
    )
    
    # Only show detailed table if verbose is enabled
    if verbose:
        print(f"\nPeriods exceeding {budget_kwh:.1f} kWh over {duration_hours} hours:")
        print("-" * 80)
        print(f"{'Date':<12} {'Excess kWh':<12} {'Daylight Hours':<15} {'Watt Shortfall':<15}")
        print("-" * 80)
        
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

def analyze_24h_usage_patterns(meter_data: MeterData) -> Dict[str, float]:
    """Analyze 24-hour usage patterns to determine battery requirements.
    
    This function analyzes the historical data to find the worst-case scenarios
    for 24-hour power outages, considering different confidence levels.
    
    Args:
        meter_data: MeterData object containing hourly readings
        
    Returns:
        Dictionary with analysis results including peak, average, and percentile values
    """
    if not meter_data.hourly_readings:
        return {}
    
    # Use the existing daily totals from the meter data
    # These are already calculated as 24-hour totals
    daily_totals = list(meter_data.daily_totals.values())
    
    if not daily_totals:
        return {}
    
    # Sort for percentile calculations
    daily_totals.sort()
    
    def percentile(values: List[float], p: float) -> float:
        """Calculate the p-th percentile using linear interpolation."""
        n = len(values)
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 >= n:
            return values[-1]
        return values[f] * (1 - c) + values[f + 1] * c
    
    return {
        'min_24h': min(daily_totals),
        'p25_24h': percentile(daily_totals, 0.25),
        'p50_24h': percentile(daily_totals, 0.50),
        'p75_24h': percentile(daily_totals, 0.75),
        'p90_24h': percentile(daily_totals, 0.90),
        'p95_24h': percentile(daily_totals, 0.95),
        'p99_24h': percentile(daily_totals, 0.99),
        'max_24h': max(daily_totals),
        'avg_24h': sum(daily_totals) / len(daily_totals),
        'total_days': len(daily_totals)
    }

def calculate_battery_recommendations(meter_data: MeterData, battery_safety_margin: float = 0.20) -> Dict[str, Dict[str, float]]:
    """Calculate battery size recommendations for different confidence levels.
    
    This function provides battery size recommendations based on historical usage
    patterns, considering different confidence levels and safety margins.
    
    Args:
        meter_data: MeterData object containing hourly readings
        battery_safety_margin: Minimum battery charge percentage to maintain (0.0-1.0, default 0.20 for 20%)
        
    Returns:
        Dictionary with battery recommendations for different confidence levels
    """
    patterns = analyze_24h_usage_patterns(meter_data)
    if not patterns:
        return {}
    
    # Safety margins for different confidence levels
    confidence_margins = {
        'conservative': 1.2,  # 20% confidence margin
        'moderate': 1.1,      # 10% confidence margin
        'aggressive': 1.05    # 5% confidence margin
    }
    
    # Calculate the battery safety factor (inverse of usable capacity)
    # If we want to keep 20% in reserve, we can only use 80% of the battery
    battery_safety_factor = 1.0 / (1.0 - battery_safety_margin)
    
    recommendations = {}
    
    for level, margin in confidence_margins.items():
        # Use different percentiles based on confidence level
        if level == 'conservative':
            base_kwh = patterns['p99_24h']  # 99th percentile
        elif level == 'moderate':
            base_kwh = patterns['p95_24h']  # 95th percentile
        else:  # aggressive
            base_kwh = patterns['p90_24h']  # 90th percentile
        
        # Apply confidence margin first
        usage_with_confidence_margin = base_kwh * margin
        
        # Then apply battery safety margin to account for minimum charge level
        recommended_kwh = usage_with_confidence_margin * battery_safety_factor
        
        # Calculate additional metrics
        peak_hourly = max(meter_data.hourly_readings.values()) if meter_data.hourly_readings else 0
        avg_hourly = sum(meter_data.hourly_readings.values()) / len(meter_data.hourly_readings) if meter_data.hourly_readings else 0
        
        # Calculate usable capacity (what can actually be used)
        usable_capacity_kwh = recommended_kwh * (1.0 - battery_safety_margin)
        
        recommendations[level] = {
            'battery_size_kwh': recommended_kwh,
            'usable_capacity_kwh': usable_capacity_kwh,
            'base_usage_kwh': base_kwh,
            'confidence_margin': margin,
            'battery_safety_margin': battery_safety_margin,
            'battery_safety_factor': battery_safety_factor,
            'peak_hourly_kw': peak_hourly,
            'avg_hourly_kw': avg_hourly,
            'confidence_percentile': 99 if level == 'conservative' else (95 if level == 'moderate' else 90),
            'days_analyzed': patterns['total_days']
        }
    
    return recommendations

def calculate_solar_production_analysis(
    meter_data: MeterData,
    panel_wattage: float = 380.0,
    total_panels: int = 20,
    solar_efficiency: float = 0.30,
    sunlight_hours: float = 5.0
) -> Dict[str, float]:
    """Calculate solar production analysis and its impact on battery requirements.
    
    Args:
        meter_data: MeterData object containing hourly readings
        panel_wattage: Power rating of each solar panel in watts
        total_panels: Total number of solar panels in the system
        solar_efficiency: Solar production efficiency (0.0-1.0, default 0.30 for 30%)
        sunlight_hours: Average hours of sunlight per day
        
    Returns:
        Dictionary with solar production analysis results
    """
    if not meter_data.hourly_readings:
        return {}
    
    # Calculate total solar capacity
    total_solar_capacity_kw = (total_panels * panel_wattage) / 1000
    
    # Calculate daily solar production
    daily_solar_production_kwh = total_solar_capacity_kw * solar_efficiency * sunlight_hours
    
    # Get daily usage statistics
    timestamps = sorted(meter_data.hourly_readings.keys())
    daily_totals = []
    
    # Calculate 24-hour totals for each day
    for i in range(0, len(timestamps) - 23, 24):
        if i + 23 < len(timestamps):
            daily_total = sum(meter_data.hourly_readings[timestamps[j]] for j in range(i, i + 24))
            daily_totals.append(daily_total)
    
    if not daily_totals:
        return {}
    
    # Calculate net usage (usage - solar production)
    net_daily_usage = [max(0, daily_usage - daily_solar_production_kwh) for daily_usage in daily_totals]
    
    # Calculate statistics
    total_days = len(daily_totals)
    days_within_budget = sum(1 for usage in net_daily_usage if usage <= 30.0)
    days_exceeding_budget = total_days - days_within_budget
    
    # Calculate percentiles for net usage
    net_daily_usage_sorted = sorted(net_daily_usage)
    p25 = net_daily_usage_sorted[int(0.25 * len(net_daily_usage_sorted))]
    p50 = net_daily_usage_sorted[int(0.50 * len(net_daily_usage_sorted))]
    p75 = net_daily_usage_sorted[int(0.75 * len(net_daily_usage_sorted))]
    p90 = net_daily_usage_sorted[int(0.90 * len(net_daily_usage_sorted))]
    p95 = net_daily_usage_sorted[int(0.95 * len(net_daily_usage_sorted))]
    p99 = net_daily_usage_sorted[int(0.99 * len(net_daily_usage_sorted))]
    
    return {
        'total_solar_capacity_kw': total_solar_capacity_kw,
        'daily_solar_production_kwh': daily_solar_production_kwh,
        'solar_efficiency': solar_efficiency,
        'sunlight_hours': sunlight_hours,
        'total_days': total_days,
        'days_within_budget': days_within_budget,
        'days_exceeding_budget': days_exceeding_budget,
        'budget_compliance_percent': (days_within_budget / total_days) * 100,
        'net_usage_stats': {
            'min': min(net_daily_usage),
            'max': max(net_daily_usage),
            'avg': sum(net_daily_usage) / len(net_daily_usage),
            'p25': p25,
            'p50': p50,
            'p75': p75,
            'p90': p90,
            'p95': p95,
            'p99': p99
        },
        'original_usage_stats': {
            'min': min(daily_totals),
            'max': max(daily_totals),
            'avg': sum(daily_totals) / len(daily_totals)
        }
    }

def calculate_solar_panel_requirements(
    inverter_capacity_kw: float = 8.0,
    inverter_derating: float = 0.20,
    inverters_needed: int = 1,
    panel_wattage: float = 380.0,
    panel_voltage: float = 48.45,
    max_inverter_voltage: float = 500.0,
    max_inverter_amperage: float = 50.0,
    mppt_inputs: int = 2,
    mppt_amperage_per_input: float = 25.0,
    panel_cost: float = 100.0,
    inverter_cost: float = 4000.0,
    battery_cost: float = 3500.0
) -> Dict[str, float]:
    """Calculate solar panel requirements for the inverter system.
    
    Args:
        inverter_capacity_kw: Peak power capacity of each inverter in kW
        inverter_derating: Derating factor for real-world conditions (0.0-1.0)
        inverters_needed: Number of inverters in the system
        panel_wattage: Power rating of each solar panel in watts
        panel_voltage: Operating voltage of each solar panel in volts
        max_inverter_voltage: Maximum input voltage per inverter in volts
        max_inverter_amperage: Maximum input amperage per inverter in amps
        mppt_inputs: Number of MPPT inputs per inverter (default: 2)
        mppt_amperage_per_input: Maximum amperage per MPPT input in amps (default: 25.0)
        panel_cost: Cost per solar panel in dollars (default: 100.0)
        inverter_cost: Cost per inverter in dollars (default: 4000.0)
        battery_cost: Cost per battery in dollars (default: 3500.0)
        
    Returns:
        Dictionary with solar panel analysis results including cost estimates
    """
    # Calculate derated inverter capacity
    derated_capacity_kw = inverter_capacity_kw * (1.0 - inverter_derating)
    
    # Calculate total MPPT amperage per inverter
    total_mppt_amperage = mppt_inputs * mppt_amperage_per_input
    
    # Calculate maximum panels per inverter based on voltage constraint
    # Each MPPT input can handle up to max_inverter_voltage
    max_panels_per_mppt_voltage = int(max_inverter_voltage / panel_voltage)
    max_panels_voltage = max_panels_per_mppt_voltage * mppt_inputs
    
    # Calculate maximum panels per inverter based on amperage constraint
    # Each MPPT input can handle mppt_amperage_per_input
    # Assuming panels are connected in series (voltage adds, current stays same)
    max_panels_per_mppt_amperage = int(mppt_amperage_per_input)  # Assuming 1A per panel
    max_panels_amperage = max_panels_per_mppt_amperage * mppt_inputs
    
    # Calculate maximum panels per inverter based on power constraint
    max_panels_power = int((derated_capacity_kw * 1000) / panel_wattage)
    
    # For MPPT systems, we need to consider that each MPPT input can handle its own voltage/amperage
    # but the total power is still limited by the inverter capacity
    max_panels_per_mppt = min(max_panels_per_mppt_voltage, max_panels_per_mppt_amperage)
    max_panels_per_inverter_mppt = max_panels_per_mppt * mppt_inputs
    
    # Take the minimum of MPPT-based calculation and power constraint
    max_panels_per_inverter = min(max_panels_per_inverter_mppt, max_panels_power)
    
    # Calculate total panels for the system
    total_panels = max_panels_per_inverter * inverters_needed
    
    # Calculate total system capacity
    total_panel_capacity_kw = (total_panels * panel_wattage) / 1000
    total_derated_capacity_kw = inverters_needed * derated_capacity_kw
    
    # Calculate system utilization
    system_utilization = total_panel_capacity_kw / total_derated_capacity_kw if total_derated_capacity_kw > 0 else 0
    
    # Calculate costs
    total_panel_cost = total_panels * panel_cost
    total_inverter_cost = inverters_needed * inverter_cost
    
    return {
        'panel_wattage': panel_wattage,
        'panel_voltage': panel_voltage,
        'max_inverter_voltage': max_inverter_voltage,
        'max_inverter_amperage': max_inverter_amperage,
        'mppt_inputs': mppt_inputs,
        'mppt_amperage_per_input': mppt_amperage_per_input,
        'total_mppt_amperage': total_mppt_amperage,
        'max_panels_per_mppt_voltage': max_panels_per_mppt_voltage,
        'max_panels_per_mppt_amperage': max_panels_per_mppt_amperage,
        'max_panels_voltage': max_panels_voltage,
        'max_panels_amperage': max_panels_amperage,
        'max_panels_power': max_panels_power,
        'max_panels_per_inverter': max_panels_per_inverter,
        'total_panels': total_panels,
        'total_panel_capacity_kw': total_panel_capacity_kw,
        'total_derated_capacity_kw': total_derated_capacity_kw,
        'system_utilization': system_utilization,
        'inverters_needed': inverters_needed,
        'panel_cost': panel_cost,
        'inverter_cost': inverter_cost,
        'battery_cost': battery_cost,
        'total_panel_cost': total_panel_cost,
        'total_inverter_cost': total_inverter_cost
    }

def calculate_inverter_requirements(meter_data: MeterData, inverter_capacity_kw: float = 8.0, inverter_derating: float = 0.20) -> Dict[str, float]:
    """Calculate inverter requirements and battery counts based on inverter capacity.
    
    Args:
        meter_data: MeterData object containing hourly readings
        inverter_capacity_kw: Peak power capacity of each inverter in kW
        inverter_derating: Derating factor for real-world conditions (0.0-1.0, default 0.20 for 20%)
        
    Returns:
        Dictionary with inverter analysis results
    """
    if not meter_data.hourly_readings:
        return {}
    
    # Get peak power requirements
    peak_power_kw = max(meter_data.hourly_readings.values())
    
    # Calculate derated inverter capacity
    derated_capacity_kw = inverter_capacity_kw * (1.0 - inverter_derating)
    
    # Calculate number of inverters needed based on derated capacity
    inverters_needed = max(1, int(peak_power_kw / derated_capacity_kw) + (1 if peak_power_kw % derated_capacity_kw > 0 else 0))
    
    # Calculate total inverter capacity (both rated and derated)
    total_rated_capacity_kw = inverters_needed * inverter_capacity_kw
    total_derated_capacity_kw = inverters_needed * derated_capacity_kw
    
    # Calculate inverter utilization based on derated capacity
    avg_power_kw = sum(meter_data.hourly_readings.values()) / len(meter_data.hourly_readings)
    avg_utilization = avg_power_kw / total_derated_capacity_kw
    peak_utilization = peak_power_kw / total_derated_capacity_kw
    
    return {
        'peak_power_kw': peak_power_kw,
        'inverter_capacity_kw': inverter_capacity_kw,
        'derated_capacity_kw': derated_capacity_kw,
        'inverter_derating': inverter_derating,
        'inverters_needed': inverters_needed,
        'total_rated_capacity_kw': total_rated_capacity_kw,
        'total_derated_capacity_kw': total_derated_capacity_kw,
        'avg_utilization': avg_utilization,
        'peak_utilization': peak_utilization,
        'avg_power_kw': avg_power_kw
    }

def calculate_battery_count_recommendations(
    meter_data: MeterData, 
    battery_safety_margin: float = 0.20,
    inverter_capacity_kw: float = 8.0,
    battery_capacity_kwh: float = 14.3,
    inverter_derating: float = 0.20,
    panel_wattage: float = 380.0,
    panel_voltage: float = 48.45,
    max_inverter_voltage: float = 500.0,
    max_inverter_amperage: float = 50.0,
    mppt_inputs: int = 2,
    mppt_amperage_per_input: float = 25.0,
    panel_cost: float = 100.0,
    inverter_cost: float = 4000.0,
    battery_cost: float = 3500.0
) -> Dict[str, Dict[str, float]]:
    """Calculate battery count recommendations based on inverter capacity and battery size.
    
    Args:
        meter_data: MeterData object containing hourly readings
        battery_safety_margin: Minimum battery charge percentage to maintain
        inverter_capacity_kw: Peak power capacity of each inverter in kW
        battery_capacity_kwh: Capacity of each battery in kWh
        
    Returns:
        Dictionary with battery count recommendations for different confidence levels
    """
    # Get battery recommendations
    battery_recs = calculate_battery_recommendations(meter_data, battery_safety_margin)
    if not battery_recs:
        return {}
    
    # Get inverter requirements
    inverter_info = calculate_inverter_requirements(meter_data, inverter_capacity_kw, inverter_derating)
    if not inverter_info:
        return {}
    
    # Get solar panel requirements
    solar_info = calculate_solar_panel_requirements(
        inverter_capacity_kw, inverter_derating, inverter_info['inverters_needed'],
        panel_wattage, panel_voltage, max_inverter_voltage, max_inverter_amperage,
        mppt_inputs, mppt_amperage_per_input, panel_cost, inverter_cost, battery_cost
    )
    
    recommendations = {}
    
    for level, rec in battery_recs.items():
        # Calculate number of batteries needed
        total_battery_capacity_needed = rec['battery_size_kwh']
        batteries_needed = max(1, int(total_battery_capacity_needed / battery_capacity_kwh) + 
                              (1 if total_battery_capacity_needed % battery_capacity_kwh > 0 else 0))
        
        # Calculate actual total capacity with the number of batteries
        actual_total_capacity = batteries_needed * battery_capacity_kwh
        actual_usable_capacity = actual_total_capacity * (1.0 - battery_safety_margin)
        
        # Calculate battery utilization
        battery_utilization = rec['usable_capacity_kwh'] / actual_usable_capacity if actual_usable_capacity > 0 else 0
        
        # Calculate costs for this configuration
        total_battery_cost = batteries_needed * battery_cost
        total_system_cost = total_battery_cost + solar_info['total_panel_cost'] + solar_info['total_inverter_cost']
        
        recommendations[level] = {
            'batteries_needed': batteries_needed,
            'battery_capacity_kwh': battery_capacity_kwh,
            'total_battery_capacity_kwh': actual_total_capacity,
            'usable_capacity_kwh': actual_usable_capacity,
            'required_capacity_kwh': rec['battery_size_kwh'],
            'required_usable_kwh': rec['usable_capacity_kwh'],
            'battery_utilization': battery_utilization,
            'inverters_needed': inverter_info['inverters_needed'],
            'total_rated_capacity_kw': inverter_info['total_rated_capacity_kw'],
            'total_derated_capacity_kw': inverter_info['total_derated_capacity_kw'],
            'peak_power_kw': inverter_info['peak_power_kw'],
            'confidence_percentile': rec['confidence_percentile'],
            'total_panels': solar_info['total_panels'],
            'panels_per_inverter': solar_info['max_panels_per_inverter'],
            'total_panel_capacity_kw': solar_info['total_panel_capacity_kw'],
            'system_utilization': solar_info['system_utilization'],
            'battery_cost': battery_cost,
            'total_battery_cost': total_battery_cost,
            'total_panel_cost': solar_info['total_panel_cost'],
            'total_inverter_cost': solar_info['total_inverter_cost'],
            'total_system_cost': total_system_cost
        }
    
    return recommendations

def print_battery_recommendations(meter_data: MeterData, battery_safety_margin: float = 0.20) -> None:
    """Print battery size recommendations for surviving 24-hour power outages.
    
    Args:
        meter_data: MeterData object containing hourly readings
        battery_safety_margin: Minimum battery charge percentage to maintain (0.0-1.0, default 0.20 for 20%)
    """
    recommendations = calculate_battery_recommendations(meter_data, battery_safety_margin)
    if not recommendations:
        print("\nInsufficient data for battery recommendations.")
        return
    
    print("\n" + "="*80)
    print("BATTERY SIZE RECOMMENDATIONS FOR 24-HOUR POWER OUTAGES")
    print("="*80)
    
    # Print analysis summary
    patterns = analyze_24h_usage_patterns(meter_data)
    print(f"\nAnalysis based on {patterns['total_days']} days of historical data:")
    print(f"24-hour usage range: {patterns['min_24h']:.1f} - {patterns['max_24h']:.1f} kWh")
    print(f"Average 24-hour usage: {patterns['avg_24h']:.1f} kWh")
    print(f"Peak hourly usage: {max(meter_data.hourly_readings.values()):.2f} kW")
    print(f"Battery safety margin: {battery_safety_margin*100:.0f}% (minimum charge to maintain)")
    
    print("\nBattery Size Recommendations:")
    print("-" * 90)
    print(f"{'Confidence Level':<15} {'Total Size':<12} {'Usable':<10} {'Base Usage':<12} {'Conf. Margin':<12} {'Coverage':<10}")
    print("-" * 90)
    
    for level, rec in recommendations.items():
        coverage = f"{rec['confidence_percentile']}%"
        print(
            f"{level.capitalize():<15} {rec['battery_size_kwh']:>11.1f} kWh "
            f"{rec['usable_capacity_kwh']:>9.1f} kWh {rec['base_usage_kwh']:>11.1f} kWh "
            f"{rec['confidence_margin']:>11.1f}x {coverage:>9}"
        )
    
    # Detailed analysis
    print("\nDetailed Analysis:")
    print("-" * 80)
    
    for level, rec in recommendations.items():
        print(f"\n{level.capitalize()} Recommendation ({rec['confidence_percentile']}% confidence):")
        print(f"  • Total battery size: {rec['battery_size_kwh']:.1f} kWh")
        print(f"  • Usable capacity: {rec['usable_capacity_kwh']:.1f} kWh (keeps {battery_safety_margin*100:.0f}% in reserve)")
        print(f"  • Based on {rec['base_usage_kwh']:.1f} kWh usage (worst {100-rec['confidence_percentile']}% of days)")
        print(f"  • Confidence margin: {(rec['confidence_margin']-1)*100:.0f}%")
        print(f"  • Peak power requirement: {rec['peak_hourly_kw']:.2f} kW")
        print(f"  • Average power consumption: {rec['avg_hourly_kw']:.2f} kW")
        
        # Calculate runtime estimates using usable capacity
        if rec['avg_hourly_kw'] > 0:
            runtime_hours = rec['usable_capacity_kwh'] / rec['avg_hourly_kw']
            print(f"  • Estimated runtime at average load: {runtime_hours:.1f} hours")
        
        if rec['peak_hourly_kw'] > 0:
            peak_runtime_hours = rec['usable_capacity_kwh'] / rec['peak_hourly_kw']
            print(f"  • Estimated runtime at peak load: {peak_runtime_hours:.1f} hours")
    
    # Additional considerations
    print("\nAdditional Considerations:")
    print("-" * 80)
    print(f"• Battery safety margin of {battery_safety_margin*100:.0f}% prevents deep discharge and extends battery life")
    print("• These recommendations assume 100% battery efficiency")
    print("• Real-world efficiency is typically 85-95%")
    print("• Consider adding 10-15% capacity for battery degradation over time")
    print("• Peak power requirements may limit usable capacity")
    print("• Consider load shedding strategies for high-power appliances")
    
    # Seasonal analysis if we have enough data
    if patterns['total_days'] >= 30:
        print("\nSeasonal Considerations:")
        print("-" * 80)
        print("• Summer months typically have higher AC usage")
        print("• Winter months may have higher heating usage")
        print("• Consider seasonal load patterns when sizing battery")
        print("• Solar generation can offset battery requirements during daylight hours")

def print_solar_production_analysis(
    meter_data: MeterData,
    panel_wattage: float = 380.0,
    total_panels: int = 20,
    solar_efficiency: float = 0.30,
    sunlight_hours: float = 5.0
) -> None:
    """Print solar production analysis and its impact on battery requirements.
    
    Args:
        meter_data: MeterData object containing hourly readings
        panel_wattage: Power rating of each solar panel in watts
        total_panels: Total number of solar panels in the system
        solar_efficiency: Solar production efficiency (0.0-1.0, default 0.30 for 30%)
        sunlight_hours: Average hours of sunlight per day
    """
    analysis = calculate_solar_production_analysis(
        meter_data, panel_wattage, total_panels, solar_efficiency, sunlight_hours
    )
    
    if not analysis:
        print("\nInsufficient data for solar production analysis.")
        return
    
    print("\n" + "="*80)
    print("SOLAR PRODUCTION ANALYSIS & BATTERY BUDGET IMPACT")
    print("="*80)
    
    # Solar system specifications
    print(f"\nSolar System Specifications:")
    print(f"Total panels: {total_panels}")
    print(f"Panel wattage: {panel_wattage:.0f}W")
    print(f"Total solar capacity: {analysis['total_solar_capacity_kw']:.1f} kW")
    print(f"Solar efficiency: {analysis['solar_efficiency']:.1%}")
    print(f"Sunlight hours per day: {analysis['sunlight_hours']:.1f}")
    print(f"Daily solar production: {analysis['daily_solar_production_kwh']:.1f} kWh")
    
    # Budget analysis comparison
    print(f"\nBattery Budget Analysis (30 kWh limit):")
    print("-" * 60)
    print(f"{'Metric':<25} {'Without Solar':<15} {'With Solar':<15} {'Improvement':<15}")
    print("-" * 60)
    print(f"{'Days within budget':<25} {analysis['total_days'] - analysis['days_exceeding_budget']:<15} {analysis['days_within_budget']:<15} {analysis['budget_compliance_percent'] - ((analysis['total_days'] - analysis['days_exceeding_budget']) / analysis['total_days'] * 100):+.1f}%")
    print(f"{'Days exceeding budget':<25} {analysis['days_exceeding_budget']:<15} {analysis['days_exceeding_budget']:<15} {analysis['budget_compliance_percent'] - ((analysis['total_days'] - analysis['days_exceeding_budget']) / analysis['total_days'] * 100):+.1f}%")
    print(f"{'Budget compliance':<25} {((analysis['total_days'] - analysis['days_exceeding_budget']) / analysis['total_days'] * 100):.1f}% {analysis['budget_compliance_percent']:>14.1f}% {analysis['budget_compliance_percent'] - ((analysis['total_days'] - analysis['days_exceeding_budget']) / analysis['total_days'] * 100):>+14.1f}%")
    print("-" * 60)
    
    # Usage statistics comparison
    print(f"\nDaily Usage Statistics:")
    print("-" * 60)
    print(f"{'Metric':<25} {'Without Solar':<15} {'With Solar':<15} {'Reduction':<15}")
    print("-" * 60)
    print(f"{'Average daily usage':<25} {analysis['original_usage_stats']['avg']:>13.1f} kWh {analysis['net_usage_stats']['avg']:>13.1f} kWh {analysis['original_usage_stats']['avg'] - analysis['net_usage_stats']['avg']:>13.1f} kWh")
    print(f"{'Peak daily usage':<25} {analysis['original_usage_stats']['max']:>13.1f} kWh {analysis['net_usage_stats']['max']:>13.1f} kWh {analysis['original_usage_stats']['max'] - analysis['net_usage_stats']['max']:>13.1f} kWh")
    print(f"{'Minimum daily usage':<25} {analysis['original_usage_stats']['min']:>13.1f} kWh {analysis['net_usage_stats']['min']:>13.1f} kWh {analysis['original_usage_stats']['min'] - analysis['net_usage_stats']['min']:>13.1f} kWh")
    print("-" * 60)
    
    # Percentile analysis
    print(f"\nNet Usage Percentiles (with solar offset):")
    print(f"25th percentile: {analysis['net_usage_stats']['p25']:.1f} kWh")
    print(f"50th percentile: {analysis['net_usage_stats']['p50']:.1f} kWh")
    print(f"75th percentile: {analysis['net_usage_stats']['p75']:.1f} kWh")
    print(f"90th percentile: {analysis['net_usage_stats']['p90']:.1f} kWh")
    print(f"95th percentile: {analysis['net_usage_stats']['p95']:.1f} kWh")
    print(f"99th percentile: {analysis['net_usage_stats']['p99']:.1f} kWh")
    
    # Key insights
    print(f"\nKey Insights:")
    print("-" * 60)
    solar_offset = analysis['original_usage_stats']['avg'] - analysis['net_usage_stats']['avg']
    print(f"• Daily solar offset: {solar_offset:.1f} kWh ({solar_offset/analysis['original_usage_stats']['avg']*100:.1f}% of average usage)")
    print(f"• Budget compliance improvement: {analysis['budget_compliance_percent'] - ((analysis['total_days'] - analysis['days_exceeding_budget']) / analysis['total_days'] * 100):+.1f} percentage points")
    print(f"• Days now within 30 kWh budget: {analysis['days_within_budget']} out of {analysis['total_days']} ({analysis['budget_compliance_percent']:.1f}%)")
    
    if analysis['budget_compliance_percent'] > 90:
        print(f"• Excellent: Solar significantly reduces battery requirements")
    elif analysis['budget_compliance_percent'] > 70:
        print(f"• Good: Solar provides substantial battery savings")
    elif analysis['budget_compliance_percent'] > 50:
        print(f"• Moderate: Solar helps but additional capacity may be needed")
    else:
        print(f"• Limited: Solar provides some benefit but battery sizing still critical")
    
    print()

def print_inverter_battery_recommendations(
    meter_data: MeterData, 
    battery_safety_margin: float = 0.20,
    inverter_capacity_kw: float = 8.0,
    battery_capacity_kwh: float = 14.3,
    inverter_derating: float = 0.20,
    panel_wattage: float = 380.0,
    panel_voltage: float = 48.45,
    max_inverter_voltage: float = 500.0,
    max_inverter_amperage: float = 50.0,
    mppt_inputs: int = 2,
    mppt_amperage_per_input: float = 25.0,
    panel_cost: float = 100.0,
    inverter_cost: float = 4000.0,
    battery_cost: float = 3500.0
) -> None:
    """Print inverter and battery count recommendations for 24-hour power outages.
    
    Args:
        meter_data: MeterData object containing hourly readings
        battery_safety_margin: Minimum battery charge percentage to maintain (0.0-1.0, default 0.20 for 20%)
        inverter_capacity_kw: Peak power capacity of each inverter in kW (default: 8.0)
        battery_capacity_kwh: Capacity of each battery in kWh (default: 14.3)
    """
    recommendations = calculate_battery_count_recommendations(
        meter_data, battery_safety_margin, inverter_capacity_kw, battery_capacity_kwh, 
        inverter_derating, panel_wattage, panel_voltage, max_inverter_voltage, max_inverter_amperage,
        mppt_inputs, mppt_amperage_per_input, panel_cost, inverter_cost, battery_cost
    )
    if not recommendations:
        print("\nInsufficient data for inverter and battery recommendations.")
        return
    
    inverter_info = calculate_inverter_requirements(meter_data, inverter_capacity_kw, inverter_derating)
    
    print("\n" + "="*80)
    print("INVERTER & BATTERY COUNT RECOMMENDATIONS FOR 24-HOUR POWER OUTAGES")
    print("="*80)
    
    # Print inverter analysis
    print(f"\nInverter Analysis:")
    print(f"Peak power requirement: {inverter_info['peak_power_kw']:.2f} kW")
    print(f"Average power consumption: {inverter_info['avg_power_kw']:.2f} kW")
    print(f"Individual inverter capacity: {inverter_capacity_kw:.1f} kW (rated)")
    print(f"Inverter derating: {inverter_derating:.1%}")
    print(f"Derated capacity per inverter: {inverter_info['derated_capacity_kw']:.1f} kW")
    print(f"Inverters needed: {inverter_info['inverters_needed']}")
    print(f"Total rated capacity: {inverter_info['total_rated_capacity_kw']:.1f} kW")
    print(f"Total derated capacity: {inverter_info['total_derated_capacity_kw']:.1f} kW")
    print(f"Peak utilization (derated): {inverter_info['peak_utilization']:.1%}")
    print(f"Average utilization (derated): {inverter_info['avg_utilization']:.1%}")
    
    # Print battery analysis
    print(f"\nBattery Analysis:")
    print(f"Individual battery capacity: {battery_capacity_kwh:.1f} kWh")
    print(f"Battery safety margin: {battery_safety_margin:.1%}")
    print(f"Usable capacity per battery: {battery_capacity_kwh * (1.0 - battery_safety_margin):.1f} kWh")
    
    # Get solar panel analysis
    solar_info = calculate_solar_panel_requirements(
        inverter_capacity_kw, inverter_derating, inverter_info['inverters_needed'],
        panel_wattage, panel_voltage, max_inverter_voltage, max_inverter_amperage,
        mppt_inputs, mppt_amperage_per_input, panel_cost, inverter_cost, battery_cost
    )
    
    # Print solar panel analysis
    print(f"\nSolar Panel Analysis:")
    print(f"Panel specifications: {panel_wattage:.0f}W @ {panel_voltage:.2f}V")
    print(f"MPPT inputs per inverter: {mppt_inputs} x {mppt_amperage_per_input:.0f}A = {solar_info['total_mppt_amperage']:.0f}A total")
    print(f"Inverter constraints: {max_inverter_voltage:.0f}V max per MPPT input")
    print(f"Max panels per MPPT input: {solar_info['max_panels_per_mppt_voltage']} (voltage limited)")
    print(f"Max panels per inverter: {solar_info['max_panels_per_inverter']} ({mppt_inputs} MPPT inputs)")
    print(f"Total panels for system: {solar_info['total_panels']}")
    print(f"Total panel capacity: {solar_info['total_panel_capacity_kw']:.1f} kW")
    print(f"System utilization: {solar_info['system_utilization']:.1%}")
    
    # Print cost summary
    print(f"\nSystem Cost Summary:")
    print(f"Solar panels: {solar_info['total_panels']} x ${panel_cost:.0f} = ${solar_info['total_panel_cost']:,.0f}")
    print(f"Inverters: {inverter_info['inverters_needed']} x ${inverter_cost:,.0f} = ${solar_info['total_inverter_cost']:,.0f}")
    print(f"Base system cost: ${solar_info['total_panel_cost'] + solar_info['total_inverter_cost']:,.0f}")
    
    # Print recommendations table
    print(f"\nBattery Count Recommendations:")
    print("-" * 130)
    print(f"{'Confidence':<12} {'Batteries':<10} {'Total kWh':<10} {'Usable kWh':<12} {'Utilization':<12} {'Confidence %':<10} {'Total Cost':<12}")
    print("-" * 130)
    
    for level, rec in recommendations.items():
        print(
            f"{level.capitalize():<12} {rec['batteries_needed']:<10} "
            f"{rec['total_battery_capacity_kwh']:<10.1f} {rec['usable_capacity_kwh']:<12.1f} "
            f"{rec['battery_utilization']:<12.1%} {rec['confidence_percentile']:<10.0f}% "
            f"${rec['total_system_cost']:,.0f}"
        )
    
    print("-" * 100)
    
    # Print detailed recommendations
    print(f"\nDetailed Recommendations:")
    print("-" * 50)
    
    for level, rec in recommendations.items():
        print(f"\n{level.capitalize()} Recommendation ({rec['confidence_percentile']:.0f}% confidence):")
        print(f"  • Batteries needed: {rec['batteries_needed']}")
        print(f"  • Total battery capacity: {rec['total_battery_capacity_kwh']:.1f} kWh")
        print(f"  • Usable capacity: {rec['usable_capacity_kwh']:.1f} kWh")
        print(f"  • Required capacity: {rec['required_capacity_kwh']:.1f} kWh")
        print(f"  • Battery utilization: {rec['battery_utilization']:.1%}")
        print(f"  • Inverters needed: {rec['inverters_needed']}")
        print(f"  • Total rated capacity: {rec['total_rated_capacity_kw']:.1f} kW")
        print(f"  • Total derated capacity: {rec['total_derated_capacity_kw']:.1f} kW")
        print(f"  • Peak power requirement: {rec['peak_power_kw']:.2f} kW")
        print(f"  • Solar panels needed: {rec['total_panels']}")
        print(f"  • Panels per inverter: {rec['panels_per_inverter']}")
        print(f"  • Total panel capacity: {rec['total_panel_capacity_kw']:.1f} kW")
        print(f"  • System utilization: {rec['system_utilization']:.1%}")
        
        # Calculate runtime estimates
        avg_runtime_hours = rec['usable_capacity_kwh'] / inverter_info['avg_power_kw'] if inverter_info['avg_power_kw'] > 0 else 0
        peak_runtime_hours = rec['usable_capacity_kwh'] / rec['peak_power_kw'] if rec['peak_power_kw'] > 0 else 0
        
        print(f"  • Estimated runtime at average load: {avg_runtime_hours:.1f} hours")
        print(f"  • Estimated runtime at peak load: {peak_runtime_hours:.1f} hours")
    
    print(f"\nSystem Configuration Summary:")
    print("-" * 50)
    print(f"Recommended configuration for {rec['confidence_percentile']:.0f}% confidence:")
    print(f"  • {rec['inverters_needed']} x {inverter_capacity_kw:.1f} kW inverters (rated)")
    print(f"  • {rec['batteries_needed']} x {battery_capacity_kwh:.1f} kWh batteries")
    print(f"  • {rec['total_panels']} x {panel_wattage:.0f}W solar panels ({rec['panels_per_inverter']} per inverter)")
    print(f"  • Total rated capacity: {rec['total_rated_capacity_kw']:.1f} kW / {rec['total_battery_capacity_kwh']:.1f} kWh")
    print(f"  • Total derated capacity: {rec['total_derated_capacity_kw']:.1f} kW")
    print(f"  • Total panel capacity: {rec['total_panel_capacity_kw']:.1f} kW")
    print(f"  • Usable energy storage: {rec['usable_capacity_kwh']:.1f} kWh")
    
    print(f"\nAdditional Considerations:")
    print("-" * 50)
    print(f"• Battery safety margin of {battery_safety_margin:.1%} prevents deep discharge")
    print(f"• These recommendations assume 100% battery efficiency")
    print(f"• Real-world efficiency is typically 85-95%")
    print(f"• Consider adding 10-15% capacity for battery degradation over time")
    print(f"• Peak power requirements may limit usable capacity")
    print(f"• Consider load shedding strategies for high-power appliances")
    print()

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
        help='Battery capacity in kWh for budget analysis (default: 30.0)'
    )
    parser.add_argument(
        '--battery-runtime-hours',
        type=int,
        default=24,
        help='Battery runtime period in hours for budget analysis (default: 24)'
    )
    parser.add_argument(
        '--hourly-summary',
        action='store_true',
        help='Print detailed hourly usage summary'
    )
    parser.add_argument(
        '--battery-recommendations',
        action='store_true',
        help='Print battery size recommendations for 24-hour power outages'
    )
    parser.add_argument(
        '--battery-safety-margin',
        type=float,
        default=0.20,
        help='Battery safety margin as decimal (0.0-1.0) to prevent deep discharge (default: 0.20 for 20%%)'
    )
    parser.add_argument(
        '--inverter-battery-analysis',
        action='store_true',
        help='Print inverter and battery count recommendations based on inverter capacity'
    )
    parser.add_argument(
        '--inverter-capacity-kw',
        type=float,
        default=8.0,
        help='Peak power capacity of each inverter in kW (default: 8.0)'
    )
    parser.add_argument(
        '--battery-capacity-kwh',
        type=float,
        default=14.3,
        help='Capacity of each battery in kWh (default: 14.3)'
    )
    parser.add_argument(
        '--inverter-derating',
        type=float,
        default=0.20,
        help='Inverter derating factor as decimal (0.0-1.0) for real-world conditions (default: 0.20 for 20%%)'
    )
    parser.add_argument(
        '--panel-wattage',
        type=float,
        default=380.0,
        help='Power rating of each solar panel in watts (default: 380.0)'
    )
    parser.add_argument(
        '--panel-voltage',
        type=float,
        default=48.45,
        help='Operating voltage of each solar panel in volts (default: 48.45)'
    )
    parser.add_argument(
        '--max-inverter-voltage',
        type=float,
        default=500.0,
        help='Maximum input voltage per inverter in volts (default: 500.0)'
    )
    parser.add_argument(
        '--max-inverter-amperage',
        type=float,
        default=50.0,
        help='Maximum input amperage per inverter in amps (default: 50.0)'
    )
    parser.add_argument(
        '--mppt-inputs',
        type=int,
        default=2,
        help='Number of MPPT inputs per inverter (default: 2)'
    )
    parser.add_argument(
        '--mppt-amperage-per-input',
        type=float,
        default=25.0,
        help='Maximum amperage per MPPT input in amps (default: 25.0)'
    )
    parser.add_argument(
        '--panel-cost',
        type=float,
        default=100.0,
        help='Cost per solar panel in dollars (default: 100.0)'
    )
    parser.add_argument(
        '--inverter-cost',
        type=float,
        default=4000.0,
        help='Cost per inverter in dollars (default: 4000.0)'
    )
    parser.add_argument(
        '--battery-cost',
        type=float,
        default=3500.0,
        help='Cost per battery in dollars (default: 3500.0)'
    )
    parser.add_argument(
        '--solar-production-analysis',
        action='store_true',
        help='Print solar production analysis and its impact on battery requirements'
    )
    parser.add_argument(
        '--solar-efficiency',
        type=float,
        default=0.30,
        help='Solar production efficiency as decimal (0.0-1.0, default: 0.30 for 30%%)'
    )
    parser.add_argument(
        '--sunlight-hours',
        type=float,
        default=5.0,
        help='Average hours of sunlight per day (default: 5.0)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed processing information'
    )
    args = parser.parse_args()
    
    # Validate battery safety margin
    if not 0.0 <= args.battery_safety_margin <= 1.0:
        print("Error: Battery safety margin must be between 0.0 and 1.0")
        return
    
    # Validate inverter derating
    if not 0.0 <= args.inverter_derating <= 1.0:
        print("Error: Inverter derating must be between 0.0 and 1.0")
        return
    
    # Validate solar efficiency
    if not 0.0 <= args.solar_efficiency <= 1.0:
        print("Error: Solar efficiency must be between 0.0 and 1.0")
        return
    
    # Load meter data
    meter_data = load_meter_data([args.xml_file], args.verbose)
    
    # Print report for each meter
    for meter_id, data in meter_data.items():
        print_meter_report(data)
        
        if args.hourly_summary:
            print_hourly_summary(data)
        
        print_budget_exceeded_periods(data, args.battery_size_kwh, args.battery_runtime_hours, args.verbose)
        
        if args.battery_recommendations:
            print_battery_recommendations(data, args.battery_safety_margin)
        
        if args.inverter_battery_analysis:
            print_inverter_battery_recommendations(
                data, 
                args.battery_safety_margin, 
                args.inverter_capacity_kw, 
                args.battery_capacity_kwh,
                args.inverter_derating,
                args.panel_wattage,
                args.panel_voltage,
                args.max_inverter_voltage,
                args.max_inverter_amperage,
                args.mppt_inputs,
                args.mppt_amperage_per_input,
                args.panel_cost,
                args.inverter_cost,
                args.battery_cost
            )
        
        if args.solar_production_analysis:
            # Get total panels from inverter analysis if available
            total_panels = 20  # Default fallback
            if args.inverter_battery_analysis:
                # Try to get panels from inverter analysis
                try:
                    inverter_info = calculate_inverter_requirements(data, args.inverter_capacity_kw, args.inverter_derating)
                    solar_info = calculate_solar_panel_requirements(
                        args.inverter_capacity_kw, args.inverter_derating, inverter_info['inverters_needed'],
                        args.panel_wattage, args.panel_voltage, args.max_inverter_voltage, args.max_inverter_amperage,
                        args.mppt_inputs, args.mppt_amperage_per_input
                    )
                    total_panels = solar_info['total_panels']
                except:
                    pass  # Use default if calculation fails
            
            print_solar_production_analysis(
                data,
                args.panel_wattage,
                total_panels,
                args.solar_efficiency,
                args.sunlight_hours
            )

if __name__ == '__main__':
    main() 