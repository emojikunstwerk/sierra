import copy
import pandas as pd 
# import http  # will lean on pd.read_csv instead
import pyArango as pr
from time import sleep
from getpass import getpass

# https://en.wikipedia.org/wiki/Droughts_in_California
# https://upload.wikimedia.org/wikipedia/commons/1/1c/Drought_area_in_California.svg

RANGE_SLICE_MONTHS = 12         # maximum window size per request
DATE_RANGE = [                  # boundaries are inclusive
    pd.Timestamp(2017, 4, 1),   # end of prior drought
    pd.Timestamp(2021, 5, 16)   # today; inc 2018 and ongoing 2020 drought
]
REQ_SLEEP           = 5         # seconds between requests to the WCIS server

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

DB_NAME          = "sierra"
DB_USER          = "sierra"
COLL_STATIONS    = "stations"
COLL_OBS         = "observation_data"
COLL_EDGE        = "observation_edges"


def load_from_web():
    c, db = db_init()

    for req_args in define_requests():
        r = load_raw(req_args) 
        write_slice_to_db(db, r)
        sleep(REQ_SLEEP)

    c.disconnectSession()   # oddly, this method is not documented
    return True


# WCIS interactions --------

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
        reqs.append(r)

    return reqs

def load_raw(args):
    url = make_url(args)

    print("requesting:", args['region'], date_range_str(args['date_range']),  "... ", end = "", flush = True)
    df = pd.read_csv(url, 
            comment = "#"
            # parse_dates = ['Date']
        ).rename(
            columns = COLUMN_NAME_MAP
        )

    df = maybe_filter_ca(args, df)

    print("done.", flush = True)
    return df

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
        date_str, "/",
        col_str,
        "?fitToScreen=false"
    ])


# DB interactions ----------

def db_init():
    c   = make_conn(DB_USER)
    db  = maybe_create_db(c, DB_NAME)
    maybe_create_collection(db, COLL_STATIONS)
    maybe_create_collection(db, COLL_OBS)
    maybe_create_collection(db, COLL_EDGE)
    db.reloadCollections()

    return (c, db)

def make_conn(user):
    return pr.connection.Connection(username = user, password = getpass())

def maybe_create_db(conn):
    if conn.hasDatabase(DB_NAME):
        return conn[DB_NAME]
    return conn.createDatabase(DB_NAME) 

def maybe_create_collection(db, collection_name):
    if db.hasCollection(collection_name):
        return db[collection_name]
    return db.createCollection(name = collection_name)

def write_slice_to_db(db, df):
    # future note: there's pr.Collection.bulkImport_* (from a file) and pr.Collection.bulkSave()
    for row in df.itertuples(index = False):
        write_doc_to_db(db, row)

    return True

def write_doc_to_db(db, row):
    s_doc = maybe_create_station_doc(db, row)

    o_doc = db[COLL_OBS].createDocument()
    o_doc.set({
        'air_temp_obs_c':           row.air_temp_obs_c,
        'air_temp_avg_c':           row.air_temp_avg_c,
        'reservoir_volume_dam3':    row.reservoir_volume_dam3,
        'precipitation_mm':         row.precipitation_mm,
        'snow_depth_cm':            row.snow_depth_cm,
        'snow_density_pct':         row.snow_density_pct,
        'snow_water_equiv_mm':      row.snow_water_equiv_mm,
        'snow_rain_ratio':          row.snow_rain_ratio
    })
    o_doc.save()

    edge = db[COLL_EDGE].createEdge()
    edge.links(o_doc, s_doc)    # _to_ the station
    edge['date'] = row.date
    edge.save()

    return True

def maybe_create_station_doc(db, row):
    s_doc = db[COLL_STATIONS].fetchDocument(row.station_id)
    if s_doc:
        return s_doc

    s_doc = db[COLL_STATIONS].createDocument()
    s_doc.set({
        'station_id':       row.station_id,
        'station_name':     row.station_name,
        'state':            row.state,
        'elevation_ft':     row.elevation_ft,
        'latitude':         row.latitude,
        'longitude':        row.longitude
    })
    s_doc.save()

    return s_doc
