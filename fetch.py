import copy
from datetime import datetime as dt
import pandas as pd 
# import http  # will lean on pd.read_csv instead
import pyarango as pyAr

# https://en.wikipedia.org/wiki/Droughts_in_California
# https://upload.wikimedia.org/wikipedia/commons/1/1c/Drought_area_in_California.svg

RANGE_SLICE_MONTHS = 12         # maximum window size per request
DATE_RANGE = [                  # boundaries are inclusive
    dt.datetime(2017, 4, 1),    # end of prior drought
    dt.datetime(2021, 5, 16)    # today; inc 2018 and ongoing 2020 drought
]

REQUEST_COLS = [
    "stationId",
    "name",
    "state.code",
    "elevation",
    "latitude",
    "longitude",
    "TOBS::value",
    "TAVG::value",
    "RESC::value",
    "PREC::value",
    "SNWD::value",
    "SNDN::value",
    "WTEQ::value",
    "SNRR::value"
]

COLUMN_NAME_MAP = {
    'Date':                                                     'date',
    'Station Id':                                               'station_id',
    'Station Name':                                             'station_name',
    'State Code':                                               'state',
    'Elevation (ft)':                                           'elevation_ft',
    'Latitude':                                                 'latitude',
    'Longitude':                                                'longitude',
    'Air Temperature Observed (degC) Start of Month Values':    'air_temp_obs_c',
    'Air Temperature Average (degC)':                           'air_temp_avg_c',
    'Reservoir Storage Volume (dam^3) Start of Month Values':   'reservoir_volume_dam3',
    'Precipitation Accumulation (mm) Start of Month Values':    'precipitation_mm',
    'Snow Depth (cm) Start of Month Values':                    'snow_depth_cm',
    'Snow Density (pct) Start of Month Values':                 'snow_density_pct',
    'Snow Water Equivalent (mm) Start of Month Values':         'snow_water_equiv_mm',
    'Snow Rain Ratio (unitless)':                               'snow_rain_ratio'
}

def load_from_web():
    for req_args in define_requests():
        load_raw(req_args) 

    return True

def define_requests(date_range = DATE_RANGE):
    requests = []

    requests += new_requests("huc", "16050101", date_range)
    requests += new_requests("huc", "16050102", date_range)
    requests += new_requests("state", "CA", date_range)

    return requests

def new_requests(region_type, region, date_range):
    reqs = []

    req = {
        'cols':         REQUEST_COLS,
        'region':       region,
        'region_type':  region_type,
        'date_range':   []
    }

    for period in pd.period_range(start = date_range[0], end = date_range[1], freq = "Y"):
        # "Better support for irregular intervals with arbitrary start and end points are forth-coming in future releases." quoth pandas docs
        floor = date_range[0] if period.start_time < date_range[0] else period.start_time
        ceil  = date_range[1] if period.end_time   > date_range[1] else period.end_time

        r = copy.copy(req)
        r['date_range'] = [floor, ceil]
        reqs.push(r)

    return reqs

def load_raw(args, do_write_to_db = True):
    url = make_url(args)

    print("requesting:", args['region'], date_range_str(args['date_range']),  "... ", end = "")
    df = pd.read_csv(url, 
            comment = "#",
            parse_dates = ['Date']
        ).rename(
            columns = COLUMN_NAME_MAP
        )

    df = maybe_filter_ca(args, df)

    if do_write_to_db:
        print("writing ... ", end = "")
        write_to_db(df)

    print("done.")
    return True

def maybe_filter_ca(args, df):
    if args["region_type"] != "state" or args["region"] != "CA":
        # for the non-CA files, we need to filter out the CA stations
        og_n = len(df)
        df   = df[df['State Code'] != "CA"]
        print("from non-CA region data, removed ", str(og_n - len(df)), " CA rows (of ", str(og_n), ")")

    return df

def date_range_str(date_range):
    return ",".join([d.strftime("%Y-%m-%d") for d in date_range])

def make_url(args):
    # see sierra-snotel-notes.txt

    region_str  = args['region_type'] + "=%22" + args['region']
    region_str  += ("*" if args['region_type'] == "huc" else "")
    region_str  += "%22"
    col_str     = ",".join(args['cols'])
    date_str    = date_range_str(args['date_range'])

    return "".join([
        "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultipleStationReport,metric/monthly/start_of_period/",
        region_str,
        "%20AND%20outServiceDate=%222100-01-01%22",
        "%7Cname/", 
        date_str,
        col_str,
        "?fitToScreen=false"
    ])

def write_to_db(df):
    return
