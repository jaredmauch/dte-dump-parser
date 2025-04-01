# dte-dump-parser
parse DTE XML files for analysis to determine if one is ready to go off-grid

How to use this tool:

First download your data here:

https://usage.dteenergy.com/download

Pick the furthest back start time you can.  Download in XML format.

How to install python and the dependencies is left as an excercise to the reader.

Lets assume you want to sustain yourself for 30 hours on a battery based on your
historical usage.  We regularly face outages at our home that exceed this time
period and even with a generator you can face the typical extended outages like
what we have experienced with over 8 days in a row where the utility voltage is
substandard or non-existant.  

Typical home generators require a full oil change after every 100-200 hours of
service, so in this case it's almost every time that we face an extended outage.

Instead of this, with the lower cost of the LiFEPO4 batteries these days such as
EG4 14.3kW I wanted a tool that would help me determine what my risk was if we
kept the generator _off_ or only on for a few hours a day instead of running
24x7x8 days.

Utility companies know when there is low voltage through the AMI Meters they
have, including the SmartWay ones that DTE uses.  This low voltage may continue
to work for some items, but garage door openers may move slowly or it can damage
your euqipment.  It's also well known that some ATS (Automatic Transfer
Switches) may partially engage which may cause them to arc and possibly cause an
electrical fire if the grid voltage is near or fluxuating near that engaement
point.

This can be unsafe if your are out of town or tending to a sick family member
and this occurs.


The following example shows what things might look like if you had 30kWh of
battery storage available and wanted to survive 12 hours without any other power
inputs.

Because if you have a grid-tied solar system that sells back to the utility you
may be subject to their system caps, I don't recommend this.


This data shows 55 days we would not be able to survive 12 hours of usage with
just 30 kWh of storage, but as you can see the shortfall on some days is quite
small, within the budget of not running a microwave.  I've added some percentile
breakdowns as well so you can use this to try and target the sweet spot based on
your own historical data.

If you are not familiar with why percentiles are an important way to review data
like this, a quick review of this data shows that at the 75th percentile you
have far less than the peak shortfall 75% of the time and just 25% of the time
you are in that case where there is excess power usage.  Since we as humans tend
to adjust our behavior based on things like a known power outage, you can expect
that even if you target the 75th percentile with either additional power
generation via solar or even a generator to add supplemental power to your
storage it won't need to run for long or often.

If you are also fetching data from another system, eg: directly from your
Powerly Energy Bridge device and storing it in something like influxdb, you may
be able to create some more detailed and accurate usage breakdowns for power
usage during daylight hours or otherwise.

These scripts should also work with any other data sources with espi xml format,
but I have no other ones to factor in.


```
./xml_usage_parser.py office_electric_usage.20240529_20250330.xml --battery-size-kwh 30 --battery-runtime-hours 12
Processing XML file...

XML File Date Range:
Start: 2024-05-29 00:00:00
End:   2025-03-30 23:00:00
Total Period: 306.0 days
Total Hourly Readings: 7343

Generating summary report...

Electric Usage Summary for Electric Data
Meter ID: YOUR_METER_UUID

Peak Usage:
Hourly: 8.82 kW
Daily:  92.32 kWh

Average Usage:
Hourly: 1.33 kW
Daily:  31.91 kWh

Period Coverage:
307 days (7342 hours)
From: 2024-05-29 00:00:00
To:   2025-03-30 23:00:00

File Coverage:
office_electric_usage.20240529_20250330.xml:
  From: 2024-05-29 00:00:00
  To:   2025-03-30 23:00:00

Recent Daily Usage:
2025-03-26: 31.38 kWh
2025-03-27: 31.17 kWh
2025-03-28: 18.06 kWh
2025-03-29: 13.76 kWh
2025-03-30: 25.09 kWh
Periods exceeding 30.0 kWh over 12 hours:
2024-06-22 - Excess: 26.43 kWh (Daylight: 8.7 hours, Watt Shortfall: 3025.5 W)
2024-06-23 - Excess: 15.83 kWh (Daylight: 8.7 hours, Watt Shortfall: 1811.0 W)
2024-06-24 - Excess: 11.94 kWh (Daylight: 8.7 hours, Watt Shortfall: 1365.6 W)
2024-06-26 - Excess: 3.20 kWh (Daylight: 8.8 hours, Watt Shortfall: 365.3 W)
2024-07-02 - Excess: 4.16 kWh (Daylight: 8.8 hours, Watt Shortfall: 472.6 W)
2024-07-03 - Excess: 14.26 kWh (Daylight: 8.8 hours, Watt Shortfall: 1617.3 W)
2024-07-04 - Excess: 10.67 kWh (Daylight: 8.8 hours, Watt Shortfall: 1207.6 W)
2024-07-05 - Excess: 9.83 kWh (Daylight: 8.8 hours, Watt Shortfall: 1110.7 W)
2024-07-06 - Excess: 7.12 kWh (Daylight: 8.9 hours, Watt Shortfall: 803.4 W)
2024-07-07 - Excess: 12.37 kWh (Daylight: 8.9 hours, Watt Shortfall: 1393.1 W)
2024-07-08 - Excess: 30.55 kWh (Daylight: 8.9 hours, Watt Shortfall: 3433.5 W)
2024-07-09 - Excess: 18.94 kWh (Daylight: 8.9 hours, Watt Shortfall: 2124.2 W)
2024-07-10 - Excess: 4.64 kWh (Daylight: 8.9 hours, Watt Shortfall: 519.4 W)
2024-07-11 - Excess: 13.50 kWh (Daylight: 9.0 hours, Watt Shortfall: 1507.3 W)
2024-07-12 - Excess: 8.60 kWh (Daylight: 9.0 hours, Watt Shortfall: 957.7 W)
2024-07-13 - Excess: 25.52 kWh (Daylight: 9.0 hours, Watt Shortfall: 2835.1 W)
2024-07-14 - Excess: 33.91 kWh (Daylight: 9.0 hours, Watt Shortfall: 3757.7 W)
2024-07-15 - Excess: 14.48 kWh (Daylight: 9.0 hours, Watt Shortfall: 1601.1 W)
2024-07-16 - Excess: 9.32 kWh (Daylight: 9.1 hours, Watt Shortfall: 1026.9 W)
2024-07-17 - Excess: 11.91 kWh (Daylight: 9.1 hours, Watt Shortfall: 1308.8 W)
2024-07-18 - Excess: 3.18 kWh (Daylight: 9.1 hours, Watt Shortfall: 348.4 W)
2024-07-23 - Excess: 11.69 kWh (Daylight: 9.3 hours, Watt Shortfall: 1260.9 W)
2024-07-24 - Excess: 11.67 kWh (Daylight: 9.3 hours, Watt Shortfall: 1254.6 W)
2024-07-25 - Excess: 2.58 kWh (Daylight: 9.3 hours, Watt Shortfall: 276.6 W)
2024-07-27 - Excess: 1.33 kWh (Daylight: 9.4 hours, Watt Shortfall: 141.8 W)
2024-07-28 - Excess: 18.90 kWh (Daylight: 9.4 hours, Watt Shortfall: 2004.1 W)
2024-07-29 - Excess: 6.78 kWh (Daylight: 9.5 hours, Watt Shortfall: 716.2 W)
2024-07-30 - Excess: 8.12 kWh (Daylight: 9.5 hours, Watt Shortfall: 854.6 W)
2024-07-31 - Excess: 14.87 kWh (Daylight: 9.5 hours, Watt Shortfall: 1559.5 W)
2024-08-01 - Excess: 13.73 kWh (Daylight: 9.6 hours, Watt Shortfall: 1435.2 W)
2024-08-02 - Excess: 15.18 kWh (Daylight: 9.6 hours, Watt Shortfall: 1580.2 W)
2024-08-03 - Excess: 9.73 kWh (Daylight: 9.6 hours, Watt Shortfall: 1009.6 W)
2024-08-04 - Excess: 10.09 kWh (Daylight: 9.7 hours, Watt Shortfall: 1042.1 W)
2024-08-05 - Excess: 9.98 kWh (Daylight: 9.7 hours, Watt Shortfall: 1026.8 W)
2024-08-07 - Excess: 11.67 kWh (Daylight: 9.8 hours, Watt Shortfall: 1191.8 W)
2024-08-08 - Excess: 2.73 kWh (Daylight: 9.8 hours, Watt Shortfall: 277.8 W)
2024-08-09 - Excess: 1.64 kWh (Daylight: 9.9 hours, Watt Shortfall: 166.1 W)
2024-08-26 - Excess: 4.66 kWh (Daylight: 10.6 hours, Watt Shortfall: 440.2 W)
2024-08-29 - Excess: 8.07 kWh (Daylight: 10.7 hours, Watt Shortfall: 752.3 W)
2024-08-30 - Excess: 13.27 kWh (Daylight: 10.8 hours, Watt Shortfall: 1231.9 W)
2024-08-31 - Excess: 8.47 kWh (Daylight: 10.8 hours, Watt Shortfall: 782.9 W)
2024-09-01 - Excess: 3.78 kWh (Daylight: 10.9 hours, Watt Shortfall: 348.3 W)
2024-09-14 - Excess: 21.13 kWh (Daylight: 12.5 hours, Watt Shortfall: 1689.6 W)
2024-09-15 - Excess: 6.70 kWh (Daylight: 12.5 hours, Watt Shortfall: 537.8 W)
2024-11-29 - Excess: 1.00 kWh (Daylight: 9.3 hours, Watt Shortfall: 107.5 W)
2024-12-05 - Excess: 5.83 kWh (Daylight: 9.2 hours, Watt Shortfall: 631.8 W)
2024-12-06 - Excess: 0.78 kWh (Daylight: 9.2 hours, Watt Shortfall: 84.7 W)
2024-12-07 - Excess: 1.27 kWh (Daylight: 9.2 hours, Watt Shortfall: 138.7 W)
2024-12-08 - Excess: 1.96 kWh (Daylight: 9.2 hours, Watt Shortfall: 213.4 W)
2024-12-10 - Excess: 5.17 kWh (Daylight: 9.1 hours, Watt Shortfall: 565.8 W)
2024-12-11 - Excess: 2.97 kWh (Daylight: 9.1 hours, Watt Shortfall: 325.2 W)
2024-12-12 - Excess: 0.83 kWh (Daylight: 9.1 hours, Watt Shortfall: 90.7 W)
2024-12-13 - Excess: 1.15 kWh (Daylight: 9.1 hours, Watt Shortfall: 126.3 W)
2025-03-01 - Excess: 1.62 kWh (Daylight: 11.3 hours, Watt Shortfall: 144.2 W)
2025-03-08 - Excess: 5.38 kWh (Daylight: 11.6 hours, Watt Shortfall: 464.7 W)

==================================================
Budget Analysis Summary
==================================================
Total days in source data: 306
Days within 30.0 kWh budget: 251 (82.0%)
Days exceeding 30.0 kWh budget: 55 (18.0%)

Watt Shortfall Statistics:
Minimum:  7.9 W
25th %:   259.5 W
Average:  800.7 W
Median:   624.7 W
75th %:   1179.5 W
90th %:   1614.1 W
95th %:   2190.0 W
Peak:     3757.7 W
==================================================
```

