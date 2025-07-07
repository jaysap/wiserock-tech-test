"""
etl_config.py

Centralized configuration for database schema definitions and API endpoints.
"""
from sqlalchemy.types import Integer, Float, TEXT, DATE, DateTime, Boolean, BigInteger

# Controls whether the API extraction process uses multiple threads. 
# Set to False to run the extraction sequentially.
USE_MULTITHREADING = False

# Maximum number of threads to use for API extraction if multithreading is enabled.
MAX_WORKERS = 10

# A list of all API endpoints provided in api.md
API_ENDPOINTS = [
    "aries_daily_capacities",
    "procount_completiondailytb",
    "wellview_job",
    "wellview_jobreport",
    "wellview_surveypoint",
    "wellview_wellheader",
    "eia_oil_price",
    # these were not explicitly mentioned in api.md, but I'm making an educated guess
    "wiserock_note",
    "wiserock_user",
]

# A nested dictionary defining the explicit schemas for each CSV and API table.
# Pandas has a tendency to incorrectly infer data types, especially when null values are present.
# After much frustration, I went full scorched earth and defined dtypes for every column.
SCHEMA_DEFINITIONS = {
    # --- ARIES ---
    "ac_property": {
        "propnum": TEXT,
        "allocid": Integer,
        "propname": TEXT,
        "apinum": TEXT,
        "nri": Float,
        "duplicate_flag": Boolean,
    },
    # --- PRO_COUNT ---
    "completiontb": {
        "merrickid": Integer,
        "wellname": TEXT,
        "completiontype": Integer,
        "producingstatus": Integer,
        "producingmethod": Integer,
        "apiwellnumber": TEXT,
        "routeid": Integer,
        "groupid": Integer,
        "divisionid": Integer,
        "fieldgroupid": Integer,
        "areaid": Integer,
        "batteryid": Integer,
        "stateid": Integer,
        "countyid": Integer,
        "ariesid": TEXT,
        "wellviewid": TEXT,
        "activeflag": Integer,
        "mmsapiwellnumber": TEXT,
        "duplicate_flag": Boolean,
    },
    "areatb": {
        "areamerrickid": Integer,
        "areaname": TEXT,
        "duplicate_flag": Boolean,
    },
    "batterytb": {
        "batterymerrickid": Integer,
        "batteryname": TEXT,
        "duplicate_flag": Boolean,
    },
    "divisiontb": {
        "divisionmerrickid": Integer,
        "divisionname": TEXT,
        "duplicate_flag": Boolean,
    },
    "fieldgrouptb": {
        "fieldgroupmerrickid": Integer,
        "fieldgroupname": TEXT,
        "duplicate_flag": Boolean,
    },
    "producingmethodstb": {
        "producingmethodmerrickid": Integer,
        "producingmethodname": TEXT,
        "duplicate_flag": Boolean,
    },
    "producingstatustb": {
        "producingstatusmerrickid": Integer,
        "producingstatusname": TEXT,
        "duplicate_flag": Boolean,
    },
    "routetb": {
        "routemerrickid": Integer,
        "routename": TEXT,
        "duplicate_flag": Boolean,
    },
    "statecountynamestb": {
        "statecode": Integer,
        "countycode": Integer,
        "countyname": TEXT,
        "duplicate_flag": Boolean,
    },
    # --- ARIES ---
    "aries_daily_capacities": {
        "well_id": TEXT,
        "date": DATE,
        "oil": Float,
        "gas": Float,
        "water": Float,
        "duplicate_flag": Boolean,
    },
    # --- PRO_COUNT ---
    "procount_completiondailytb": {
        "merrickid": Integer,
        "recorddate": DATE,
        "productiondate": DATE,
        "producingmethod": Integer,
        "dailydowntime": Float,
        "allocestoilvol": Float,
        "oilproduction": Float,
        "allocestgasvolmcf": Float,
        "allocestinjgasvolmcf": Float,
        "allocestwatervol": Float,
        "waterproduction": Float,
        "chokesize": Float,
        "casingpressure": Float,
        "tubingpressure": Float,
        "duplicate_flag": Boolean,
    },
    # --- WELLVIEW ---
    "wellview_job": {
        "idwell": TEXT,
        "idrec": TEXT,
        "dttmend": DateTime,
        "dttmstart": DateTime,
        "jobsubtyp": TEXT,
        "jobtyp": TEXT,
        "status1": TEXT,
        "status2": TEXT,
        "targetform": TEXT,
        "usertxt1": TEXT,
        "wvtyp": TEXT,
        "duplicate_flag": Boolean,
    },
    "wellview_jobreport": {
        "idwell": TEXT,
        "idrecparent": TEXT,
        "idrec": TEXT,
        "condhole": TEXT,
        "condlease": TEXT,
        "condroad": TEXT,
        "condwave": TEXT,
        "condweather": TEXT,
        "condwind": TEXT,
        "depthtvdendprojmethod": TEXT,
        "dttmend": DateTime,
        "dttmstart": DateTime,
        "rigtime": Float,
        "duplicate_flag": Boolean,
    },
    "wellview_surveypoint": {
        "well_id": TEXT,
        "measured_depth_ft": Float,
        "inclination": Float,
        "azimuth": Float,
        "tvd": Float,
        "easting": Float,
        "northing": Float,
        "duplicate_flag": Boolean,
    },
    "wellview_wellheader": {
        "idwell": TEXT,
        "area": TEXT,
        "basin": TEXT,
        "basincode": Integer,
        "country": TEXT,
        "county": TEXT,
        "currentwellstatus1": TEXT,
        "district": TEXT,
        "dttmfirstprod": DateTime,
        "dttmspud": DateTime,
        "fieldname": TEXT,
        "fieldoffice": TEXT,
        "latitude": Float,
        "latlongdatum": TEXT,
        "lease": TEXT,
        "longitude": Float,
        "operated": Integer,
        "operator": TEXT,
        "padname": TEXT,
        "stateprov": TEXT,
        "wellconfig": TEXT,
        "wellida": TEXT,  # API well number
        "spuddate": DATE,
        "wellname": TEXT,
        "duplicate_flag": Boolean,
    },
    # --- EIA ---
    "eia_oil_price": {
        "period": DATE,
        "value": Float,
        "duplicate_flag": Boolean,
    },
    # --- WISE ROCK ---
    "wiserock_note": {
        "id": TEXT,
        "event_type": TEXT,
        "event_id": BigInteger,
        "user_id": Integer,
        "user_overwrite": TEXT,
        "note_timestamp": DateTime,
        "note_text": TEXT,
        "is_edited": Boolean,
        "event_uuid": TEXT,
        "note_id": TEXT,
        "duplicate_flag": Boolean,
    },
    "wiserock_user": {
        "user_id": Integer,
        "handle": TEXT,
        "duplicate_flag": Boolean,
    },
}

# Create CSV load dtype dict by iterating over SCHEMA_DEFINITIONS, 
CSV_LOAD_DTYPE_DICT = {
    table_name: {
        col_name: str
        for col_name, col_type in columns.items()
        if col_type in (TEXT, DATE, DateTime)
    }
    for table_name, columns in SCHEMA_DEFINITIONS.items()
}