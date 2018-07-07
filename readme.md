# Power Outage Analysis
This script performs web scraping of specified Virginia utility provider websites for reported customer power outages from automated web services.  The general workflow for the script is as follows:

- Build well-formed date strings based on current time (time the script is initiated)

- Generate URLs with the created date strings and send test request
  - Utility provider systems sometimes produce non-standard datestrings which is what necessitates this step

- If a URL returns a 200 code, outage data is pulled
  - Not all provider systems use the same format though in general outage data are stored as JSON or a JS file

- Power outage data is parsed and written to a CSV file (for monitoring) as well as two VDEM GIS SDE Feature Classes
  - Operational/Power_Outages_by_Provider_and_Locality
  - Operational/Power_Outages_by_Locality

This script is run on a windows task scheduler.  Data are updated every 15 minutes at: {hh}:{02}, {hh}:{17}, {hh}:{32}, and {hh}:{47}.  Data may take up to two minutes to reflect changes after  each time interval mentioned above occurs.

Current Data Sources:
- [DOM](http://outagemap.dom.com.s3-website-us-east-1.amazonaws.com/external/report.html?report=report-panel-county-muni)
- [AEP](http://outagemap.appalachianpower.com.s3.amazonaws.com/external/report.html?report=report-panel-county)
- [VMDAEC](http://www.outages.vmdaec.com/)

# Usage
The feature classes that receive updates from this script are pushed out as web services and consumed in an AGOL hosted Operations Dashboard.

![Ops Dashboard](https://i.imgur.com/5l8EPPF.jpg)