# -*- coding: utf-8 -*-
"""Outage Analysis

This script performs web scraping of specified Virginia utility provider websites for
reported customer power outages from automated web services.  The general workflow for the
script is as follows:

    * Build well-formed date strings based on current time (time the script is initiated)

    * Generate URLs with the created date strings and send test request
        - Utility provider systems sometimes produce non-standard datestrings which is what
        necessitates this step

    * If a URL returns a 200 code, outage data is pulled
        - Not all provider systems use the same format though in general outage data are
        stored as JSON or a JS file

    * Power outage data is parsed and written to a CSV file (for monitoring) as well as two
    VDEM GIS SDE Feature Classes
        - Operational/Power_Outages_by_Provider_and_Locality
        - Operational/Power_Outages_by_Locality

This script is run on a windows task scheduler.  Data are updated every 15 minutes at:
{hh}:{02}, {hh}:{17}, {hh}:{32}, and {hh}:{47}.  Data may take up to two minutes to
reflect changes after each time interval mentioned above occurs.

Current Data Sources:
    * DOM: http://outagemap.dom.com.s3-website-us-east-1.amazonaws.com/external/report.html?report=report-panel-county-muni
    * AEP: http://outagemap.appalachianpower.com.s3.amazonaws.com/external/report.html?report=report-panel-county
    * VMDAEC: http://www.outages.vmdaec.com/ 



"""
# Import modules
import json
import os
import urllib2
import csv
import datetime
import sys
import arcpy

# Constants
THIS_FILE_PATH = os.path.split(__file__)[0]
THIS_SCRIPT_NAME = os.path.split(__file__)[1].replace(".py", "")
DATE_STRING = str(datetime.datetime.now()).replace("//", "")
DOM = "DOM"
AEP = "AEP"

# Turn off Arcpy Logging
arcpy.SetLogHistory(False)

# Setup helper
script_start = datetime.datetime.now()
print 'Started - ' + str(script_start)

def main():
    """Main method."""
    # VMDAEC Co-op URL is static
    coop_url = "http://outages.vmdaec.com/data.js"

    # SDE Feature Classes
    fc = "" # Path to feature class
    loc_fc = "" # Path to feature class
    time_series_fc_long = "" # Path to feature class
    time_series_fc = "" # Path to feature class
    regional_fc = "" # Path to feature class

    # Temp CSV file for monitoring output
    csv_file_path = os.path.join(THIS_FILE_PATH, "CSV_Output.csv")

    # Generate URLs for dynamic sites: DOM, AEP, {others...}
    h.log("Producing DOM and AEP URLs...")
    try:
        dom_url = get_current_provider_url(DOM)
        aep_url = get_current_provider_url(AEP)
        print dom_url + "\n" + aep_url
    except Exception as error:
        # Abort - If this fails, try again in another 15 min or initiate script manually
        # Likely issue: sluggishness on the part of power provider website update and url
        # does not exist yet.
        print str(error)
        sys.exit()

    ## Main routine
    # Scrape and add to in-memory list variables
    h.log("Scraping data from websites...")
    try:
        dom_rows = scrape_dom_data(dom_url)
        aep_rows = scrape_aep_data(aep_url)
        coop_rows = scrape_coop_data(coop_url)
    except Exception as error:
        # Abort
        # If this fails, it is possible that the providers changed their JSON schema
        print str(error)
        sys.exit()

    # Create CSV
    print "Creating CSV output..."
    try:
        # Write rows from above to CSV file
        create_csv_schema(csv_file_path)
        append_csv(csv_file_path, (dom_rows + aep_rows + coop_rows))
    except IOError as error:
        # OK if this fails as it is purely for monitoring.
        # Common error: File is open/locked
        print str(error)

    # Append to provider and locality feature class
    try:
        # Copy old values to last reported
        print "Copying to providers - last reported..."
        copy_field(fc)

        # Append those same rows to a Feature Class
        print "Appending to Feature Class - Provider and Localities..."
        append_features(fc, (dom_rows + aep_rows + coop_rows))

        # Calculate delta
        print "Calculating providers - outage delta..."
        calc_delta(fc)
    except Exception as error: # TODO need to test to get targeted Error
        print str(error)

    # Append to locality feature class
    try:
        # Copy old values to last reported
        print "Copying to localities - last reported..."
        copy_field(loc_fc)

        # Append those same rows to a Feature Class
        print "Appending to Feature Class - Localities..."
        loc_dict = dissolve_list((dom_rows + aep_rows + coop_rows))
        append_localities(loc_fc, loc_dict)

        # Calculate delta
        print "Calculating localities - outage delta..."
        calc_delta(loc_fc)
    except Exception as error: # TODO need to test to get targeted Error
        print str(error)

    # dissolve_list(dom_rows + aep_rows + coop_rows)

    # Time series
    try:
        print "Appending to Table - Time Series..."
        add_time_series(loc_fc, time_series_fc)
    except Exception as error: # TODO need to test to get targeted Error
        print str(error)
    
    # Time series long
    try:
        print "Appending to Table - Time Series Long..."
        add_time_series_long(loc_fc, time_series_fc_long)
    except Exception as error: # TODO need to test to get targeted Error
        print str(error)
    
    # Append to regional feature class
    try:
        # Copy old values to last reported
        print "Copying to regional - last reported..."
        copy_field(regional_fc)

        # Append those same rows to a Feature Class
        print "Appending to Feature Class - Regional..."
        # loc_dict = dissolve_list((dom_rows + aep_rows + coop_rows))
        append_localities(regional_fc, loc_dict)

        # Calculate delta
        print "Calculating regional - outage delta..."
        calc_delta(regional_fc)
    except Exception as error: # TODO need to test to get targeted Error
        print str(error)

    script_end = datetime.datetime.now()
    delta = script_end - script_start
    print 'Finished - ' + str(script_end)
    print str(delta)


def dissolve_list(in_list):
    """Dissolve an input list.

    Takes an input list of lists (specifically formatted by the "scrape" functions in this
    module) and dissolves by unique locality name while summing the outage values for each.
    Returns a dictionary for later use in the script.

    Args:
        in_list (list): List of lists generated from the "scrape" functions.

    Returns:
        dict: Containing unique localities as the key and outages as the value.

    """
    loc_dict = {}
    localities = []

    # Create a list of all locality names
    for row in in_list:
        localities.append(row[0].lower().replace("'", ""))

    # Make the list unique
    loc_unique = set(localities)

    # Add to dict
    for loc in loc_unique:
        loc_dict[loc] = 0

    # Cycle through original list and add values
    for row in in_list:
        key = row[0].lower().replace("'", "")
        loc_dict[key] = int(loc_dict[key]) + int(row[4])

    return loc_dict

def get_current_provider_url(prov):
    """Attempt to retrieve a valid URL from a provider.

    Calls datestring/URL building functions and initiates HTTP response tests.  Once a URL
    is verified as valid it will be returned to the main function.

    Args:
        prov (string): One of two (at the moment) constants representing utility providers
        with common automated outage systems (DOM and AEP at the moment).

    Returns:
        string: Valid URL for a specified utility provider's web resource (else the script
        will fail).

    """
    # LocaL var
    url_obtained = False
    url_string = None
    date_strings = build_date_strings()

    # Cycle through date strings created above (exhaustive list) and find 200 code
    for date_string in date_strings:
        url_string = build_url(prov, date_string)
        print url_string
        code = test_url(url_string)

        if code == 200:
            # Successful URL, break the loop
            url_obtained = True
            break

    # If a web reosurce responds 200 return that URL, else the script will fail
    if url_obtained:
        return url_string
    else:
        print "No URL match found."

def build_date_strings():
    """Generate a well-formed datestring based current time.

    Currently based on AEP and DOM automated outage reporting systems.  Each provider uses
    an AWS hosted web resource that updates every 15 min. with outage numbers.  The URL to
    this resource updates as well and is not always the same though it is somewhat
    predictable.  Therefore some testing is done to ensure a working URL, else the script
    will exit and write to the log file.

    Args:
        None

    Returns:
        list: A list of well formed datestrings (len=4) in the format:
        "{yy}_{mm}_{dd}_{hh}_{minmin}{modmod}" where the modifier (mod) is either "00" or
        "01".

    """
    # Used later in for loop scope
    date_strings = []
    mods = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

    # Whole structure yields 4 loops that populate the date_strings list with 4 date strs
    for mod in mods:
        date_obj = datetime.datetime.utcnow()
        year = '%04d' % date_obj.year
        month = '%02d' % date_obj.month
        day = '%02d' % date_obj.day
        hour = '%02d' % date_obj.hour
        minute = date_obj.minute
        str_minute = ""
        modifiers = ["00", "30"]

        # This "x + mod" convention is due to the DOM and AEP systems occasionally adding
        # a 1 to their URL.  E.g. they do not always update on {hh}:00, {hh}:15, etc. and
        # instead sometimes update on {hh}:01, {hh}:16, etc.
        for modifier in modifiers:
            if minute >= (45 + mod):
                str_minute = '%02d' % (45 + mod)
            elif minute >= (30 + mod) and minute < (45 + mod):
                str_minute = '%02d' % (30 + mod)
            elif minute >= (15 + mod) and minute < (30 + mod):
                str_minute = '%02d' % (15 + mod)
            else:
                str_minute = '%02d' % (0 + mod)

            # Assemble the well-formed dat string
            date_strings.append("{0}_{1}_{2}_{3}_{4}_{5}".format(year,
                                                                 month,
                                                                 day,
                                                                 hour,
                                                                 str_minute,
                                                                 modifier))

    return date_strings

def test_url(url_string):
    """Sends an HTTP request and checks the response.

    The code is returned, else the script exits and writes to the log.

    Args:
        url_string (string): An input URL to be tested over HTTP.

    Returns:
        int: The response code.
    TODO
    """
    # Check web resource response code
    try:
        req = urllib2.urlopen(url_string)
        code = req.getcode()
    except urllib2.HTTPError as e:
        code = 404

    return code

def build_url(provider, date_string):
    """Generates a well-formed URL for a specified utility provider's outage system.

    Currently only developed for DOM and AEP as they have dynamic URLs that require
    alteration based on time of day.  Possible to add other providers here as well as the
    need arises.

    Args:
        provider (string): Constant.  At the moment either "DOM" or "AEP".
        date_string (string): The automatic datestring created by build_date_strings().

    Retruns:
        string: A well-formed URL for a utility provider's automated outage system.

    """
    # Concatenate URL with date string and provider specific URL. This could change.
    if provider == DOM:
        # url_string = "http://outagemap.dom.com.s3-website-us-east-1.amazonaws.com/resources/data/external/interval_generation_data/{0}/report_region.json".format(date_string)
        url_string = "http://outagemap.dominionenergy.com.s3.amazonaws.com/resources/data/external/interval_generation_data/{0}/report_region.json".format(date_string)
        return url_string
    elif provider == AEP:
        url_string = "http://outagemap.appalachianpower.com.s3.amazonaws.com/resources/data/external/interval_generation_data/{0}/report_county.json".format(date_string)

        return url_string
    else:
        # Potential for other feeds as needed
        pass

def append_features(fc, rows):
    """Appends outage data scraped from web resources to a feature class.

    This function is used for updateing outage numbers in the Locality/Provider union
    feature class as descirbed above.  The outage field gets set to -9999 and the last
    updated field gets set to null.  Then the reported outages are written to those fields.
    Fields left with a -9999 or a null did not have any matching data reported by the
    utility provider websites.

    Data are joined to this feature class based on a composite key of provider name and
    locality name (all lower case with special characters removed). Some locality names had
    to be manually modified in the feature class to allow for a smooth join.

    Args:
        fc (string): SDE Feature Class URL.  Specifically, this should be the locality
        and provider union dataset.

        rows (list): List of lists containing data scraped from utility provider web services.

    Returns:
        void

    """
    # Fields for FC - subject to change
    key_field = "Key_Auto"
    cust_out_field = "Cust_Out_Auto"
    date_field = "Last_Updated_Auto"
    fields = [key_field, cust_out_field, date_field]
    date = datetime.datetime.utcnow()

    # Nullify existing cust_out field
    arcpy.CalculateField_management(fc, cust_out_field, """-9999""", "PYTHON_9.3")
    arcpy.CalculateField_management(fc, date_field, """None""", "PYTHON_9.3")

    # Update fields
    for row in rows:
        key = row[2]
        cust_out = row[4]
        expression = """{0} = '{1}'""".format(key_field, key)

        with arcpy.da.UpdateCursor(fc, fields, where_clause=expression) as cursor:
            for feature in cursor:
                feature[1] = cust_out
                feature[2] = date
                cursor.updateRow(feature)
                break

def append_localities(fc, rows):
    """Appends outage data scraped from web resources to a feature class.

    Behaves similar to append_features but is specifically made to work with the locality
    only dataset.  The main difference is the way the key field is generated and joined.

    Data are joined to this feature class based on a key of locality name (all lower case 
    with special characters removed). Some locality names had to be manually modified in
    the feature class to allow for a smooth join.

    Args:
    fc (string): SDE Feature Class URL.  Specifically, this should be the locality
    and provider union dataset.

    rows (list): List of lists containing data scraped from utility provider web services.

    Returns:
        void

    """
    key_field = "Loc_Name_Auto"
    cust_out_field = "Cust_Out_Auto"
    date_field = "Last_Updated_Auto"
    human_read_field = "Cust_Out_Human_Readable"
    fields = [key_field, cust_out_field, date_field, human_read_field]
    date = datetime.datetime.utcnow()

    # Nullify existing cust_out field
    arcpy.CalculateField_management(fc, cust_out_field, """-9999""", "PYTHON_9.3")
    arcpy.CalculateField_management(fc, date_field, """None""", "PYTHON_9.3")

    # Update fields
    for dict_key in rows.keys():
        key = dict_key
        cust_out = rows[dict_key]
        expression = """{0} = '{1}'""".format(key_field, key)

        with arcpy.da.UpdateCursor(fc, fields, where_clause=expression) as cursor:
            for feature in cursor:
                feature[1] = cust_out
                feature[2] = date
                if feature[1] == -9999:
                    feature[3] = 0
                else:
                    feature[3] = cust_out
                cursor.updateRow(feature)
                break

    # expression = """!{0}!.replace(-9999, 0)""".format(cust_out_field)
    # arcpy.CalculateField_management(fc, human_read_field, expression, "PYTHON_9.3")


def create_csv_schema(file_name):
    """Creates a CSV file with a hard coded schema.

    Creates a CSV file using the input file_name and adds a hard coded schema.

    Args:
        file_name(string): File path for output CSV file.

    Returns:
        void

    """
    with open(file_name, 'wb') as csv_file:
        writer = csv.writer(csv_file, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(['LOCALITY', 'PROVIDER', 'KEY', 'LOC_CUST_SERVED',
                         'LOC_OUT', 'DATE_PULLED'])

def append_csv(file_name, rows):
    """Populate CSV file with data from rows.

    Used as a "sanity check" to compare what gets written to this CSV and the SDE feature
    classes.

    Args:
        file_name (string): FIle path to CSV file.
        rows (list): List of lists containing outage data.

    Returns:
        void

    """
    with open(file_name, 'ab') as csv_file:
        writer = csv.writer(csv_file, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)

        for row in rows:
            writer.writerow(row)

def create_prog_key(company, locality):
    """Formats input into a well-formed key for use in this script.

    Combination of locality and provider name; all lowercase, remove special characters.
    Some keys had to be modified in the SDE feature classes in order to allow for a smooth
    join.

    Args:
        company (string): Provider name gathered from provider automated system.
        locality (string): Locality name gathered from provider automated system.

    Returns:
        string: The join key.

    """
    # Characters to remove: ' . {space} / -
    characters = ["'", " ", ".", "-", "/"]
    new_string = (company + locality).lower()
    for character in characters:
        new_string = new_string.replace(character, "")

    return new_string

def scrape_dom_data(in_url):
    """Scrape Dominion VA Power data from web resource.

    Use utility functions in this script to make calls to an automated web resource for
    power outage data.  Parse the resulting JSON and put into a list of lists data
    structure.  Specifically made to work with DOM data.

    Args:
        in_url (string): The URL to parse.

    Returns:
        list: List of lists containing "rows" of outage data.

    """
    rows = []
    provider_site = "Dominion Virginia Power"
    response = urllib2.urlopen(urllib2.Request(in_url))
    json_file = json.load(response)

    regions_json = json_file["file_data"]["areas"][0]["areas"]

    for region_i in regions_json:
        region = region_i["area_name"]
        areas = region_i["areas"]

        for area in areas:
            locality = area["area_name"]
            loc_cust_served = area["cust_s"]
            loc_out = area["cust_a"]["val"]
            key = create_prog_key(provider_site, locality)

            # Create row and add to rows array
            row = [locality, provider_site, key, loc_cust_served, loc_out, DATE_STRING]
            rows.append(row)

    # Return rows
    return rows

def scrape_aep_data(in_url):
    """Scrape AEP data from web resource.

    Use utility functions in this script to make calls to an automated web resource for
    power outage data.  Parse the resulting JSON and put into a list of lists data
    structure.  Specifically made to work with AEP data.

    Args:
        in_url (string): The URL to parse.

    Returns:
        list: List of lists containing "rows" of outage data.

    """
    rows = []
    provider_site = "American Electric Power"
    response = urllib2.urlopen(urllib2.Request(in_url))
    json_file = json.load(response)

    regions_json = json_file["file_data"]["areas"][0]["areas"]

    for region in regions_json:
        rgn = region["area_name"]
        areas = region["areas"]

        for area in areas:
            locality = area["area_name"]
            loc_cust_served = area["cust_s"]
            loc_out = area["cust_a"]["val"]
            key = create_prog_key(provider_site, locality)

            row = [locality, provider_site, key, loc_cust_served, loc_out, DATE_STRING]
            rows.append(row)

    # Return rows
    return rows

def scrape_coop_data(in_url):
    """Scrape VMDAEC data from web resource.

    Use utility functions in this script to make calls to an automated web resource for
    power outage data.  Parse the resulting JS file and put into a list of lists data
    structure.  Specifically made to work with VMDAEC data.

    Args:
        in_url (string): The URL to parse.

    Returns:
        list: List of lists containing "rows" of outage data.

    """
    rows = []
    response = urllib2.urlopen(urllib2.Request(in_url))
    js_file = response.readlines()

    # Hard coded data var - may need to change if variables structure changes
    # Have to remove JS var declaration and add a bit of code to make a valid JSON
    data_var = '{ "companies": [' + js_file[0].replace("var data = ", "")[1:-4] + ']}'
    json_file_data = json.loads(data_var)

    # Hard coded coop var - may need to change if variables structure changes
    # Have to remove JS var declaration and add a bit of code to make a valid JSON
    coop_var = js_file[1].replace("var coop_data = ", "")[:-3]
    json_file_coop = json.loads(coop_var)
    # print json_file_coop

    for val in json_file_coop.values():
        # print val
        try:
            provider = val["company"]
            counties = val["county"]
            served = 0

            for county in counties:
                locality = county["name"]
                loc_out = county["outage"]
                key = create_prog_key(provider, locality)
                row = [locality, provider, key, served, loc_out, DATE_STRING]
                rows.append(row)
        except:
            continue

    # Return rows
    return rows

def copy_field(fc):
    """Copy values from a specified input field to another field.

    Args:
        fc (string): Path to feature class.

    Returns:
        void

    """
    in_field = "Cust_Out_Auto"
    out_field = "Cust_Out_Last_Report"
    expression = """!{0}!""".format(in_field)

    # Nullify existing
    arcpy.CalculateField_management(fc, out_field, """None""", "PYTHON_9.3")

    # Copy field
    arcpy.CalculateField_management(fc, out_field, expression, "PYTHON_9.3")

def calc_delta(fc):
    """Calculate the delta between two integer fields.

    Args:
        fc (string): Path to feature class.

    Returns:
        void

    """
    current = "Cust_Out_Auto"
    last = "Cust_Out_Last_Report"
    delta = "Delta_Out_Current_Last"
    fields = [current, last, delta]
    expression = """NOT {0} = -9999""".format(current)

    # Nullify existing
    arcpy.CalculateField_management(fc, delta, 0, "PYTHON_9.3")

    # Calculate delta
    with arcpy.da.UpdateCursor(fc, fields, expression) as cursor:
        for row in cursor:
            # delta = current - last
            row[2] = row[0] - row[1]
            cursor.updateRow(row)

def add_time_series(copy_from, copy_to):
    """
    TODO
    """
    # This function may not be necessary
    # Add shape xy token to fields
    # create FC with same schema plus points
    # update fc ref above
    # in insert cursor add 0,0
    # Update Purge
    # test
    fields = ["STATEFP", "COUNTYFP", "COUNTYNS", "AFFGEOID", "GEOID", "NAME", "STCOFIPS",
              "Loc_Name_Auto", "Cust_Out_Auto", "Last_Updated_Auto", "Cust_Out_Last_Report",
              "Delta_Out_Current_Last"]
    fields_shp = ["SHAPE@XY", "STATEFP", "COUNTYFP", "COUNTYNS", "AFFGEOID", "GEOID", "NAME", "STCOFIPS",
              "Loc_Name_Auto", "Cust_Out_Auto", "Last_Updated_Auto", "Cust_Out_Last_Report",
              "Delta_Out_Current_Last"]

    with arcpy.da.SearchCursor(copy_from, fields) as from_cursor:
        with arcpy.da.InsertCursor(copy_to, fields_shp) as to_cursor:
            for row in from_cursor:
                to_cursor.insertRow(((0.0, 0.0), row[0], row[1], row[2], row[3], row[4], row[5],
                                     row[6], row[7], row[8], row[9], row[10], row[11]))

def add_time_series_long(copy_from, copy_to):
    """
    TODO
    """
    # This function may not be necessary
    # Add shape xy token to fields
    # create FC with same schema plus points
    # update fc ref above
    # in insert cursor add 0,0
    # Update Purge
    # test
    fields = ["STATEFP", "COUNTYFP", "COUNTYNS", "AFFGEOID", "GEOID", "NAME", "STCOFIPS",
              "Loc_Name_Auto", "Cust_Out_Auto", "Last_Updated_Auto", "Cust_Out_Last_Report",
              "Delta_Out_Current_Last"]

    with arcpy.da.SearchCursor(copy_from, fields) as from_cursor:
        with arcpy.da.InsertCursor(copy_to, fields) as to_cursor:
            for row in from_cursor:
                to_cursor.insertRow((row[0], row[1], row[2], row[3], row[4], row[5],
                                     row[6], row[7], row[8], row[9], row[10], row[11]))



if __name__ == '__main__':
    main()
