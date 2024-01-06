'''
Future Flight Planner and Individual Flight Tracker Code
Written By: Simran Padam, Harsh Benahalkar and Shriniket Buche
'''

# Importing Libraries
from FlightRadar24 import FlightRadar24API
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, udf, size
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, ArrayType, BooleanType
import json
from pyspark.sql import SparkSession
import regex as re
import math
import time
from scipy import stats
import numpy as np
import openmeteo_requests
from math import radians, sin, cos, sqrt, atan2
import pytz

# Setting up Spark Session
spark = SparkSession.builder.appName("example").getOrCreate()

# Set current working directory as location to download auxiliary files
# May need to change for windows
file_path = './'
folder_path = os.path.join('./', "dat_files")

# Set Airport Dropdown options based on Top 50 most busiest airprots
airport_dict = {
    "Hartsfield-Jackson Atlanta International": "ATL",
    "Dallas/Fort Worth International": "DFW",
    "Denver International": "DEN",
    "O'Hare International": "ORD",
    "Dubai International": "DXB",
    "Los Angeles International": "LAX",
    "Istanbul": "IST",
    "London Heathrow": "LHR",
    "Indira Gandhi International": "DEL",
    "Charles de Gaulle": "CDG",
    "John F. Kennedy International": "JFK",
    "McCarran International": "LAS",
    "Amsterdam Schiphol": "AMS",
    "Miami International": "MIA",
    "Adolfo Suárez Madrid–Barajas": "MAD",
    "Haneda": "HND",
    "Orlando International": "MCO",
    "Frankfurt": "FRA",
    "Charlotte Douglas International": "CLT",
    "Benito Juárez International": "MEX",
    "Seattle-Tacoma International": "SEA",
    "Phoenix Sky Harbor International": "PHX",
    "Newark Liberty International": "EWR",
    "San Francisco International": "SFO",
    "Barcelona–El Prat": "BCN",
    "George Bush Intercontinental": "IAH",
    "Soekarno–Hatta International": "CGK",
    "Chhatrapati Shivaji Maharaj International": "BOM",
    "Toronto Pearson International": "YYZ",
    "Logan International": "BOS",
    "Hamad International": "DOH",
    "El Dorado International": "BOG",
    "São Paulo/Guarulhos–Governador André Franco Montoro International": "GRU",
    "Tan Son Nhat International": "SGN",
    "Gatwick": "LGW",
    "Singapore Changi": "SIN",
    "Fort Lauderdale–Hollywood International": "FLL",
    "King Abdulaziz International": "JED",
    "Munich": "MUC",
    "Antalya": "AYT",
    "Sabiha Gökçen International": "SAW",
    "Minneapolis–Saint Paul International": "MSP",
    "Cancún International": "CUN",
    "Ninoy Aquino International": "MNL",
    "Jeju International": "CJU",
    "Leonardo da Vinci–Fiumicino": "FCO",
    "Paris Orly": "ORY",
    "Sydney Kingsford Smith": "SYD",
    "LaGuardia": "LGA",
    "Suvarnabhumi": "BKK"
}

# Define Airport Dropdown
airport_options = [{'label': key, 'value': value}
                   for key, value in airport_dict.items()]

# Initialize Flight API
flight_api = FlightRadar24API()

# Set upper limit of lat/long points along flight path
# This is to make sure that there is no rate limit issue with
# weather API
DISTANCE_THRESHOLD = 10

# Flags for internal functions
DEPLOYED = True
DEBUG = False
MAX_TRIES = 3

# Count number of API calls, for internal debugging
COUNTS = 0

# =============================================================================#
# Code for Future Flight Planner
# =============================================================================#
# Set up debugging logs
log_file_path = os.path.join('./', "app_logs.txt")

# Flight schedule is stored here
global flights_data
API_KEY = "be66466a4ba80c5a616f7efabf9674c7"

files = ["routes.dat", "airports.dat",
         "airlines.dat", "planes.dat", "countries.dat"]

# Set up schema for flight schedule
json_schema = StructType([
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),
    StructField("departure", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("delay", StringType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ])),
    StructField("arrival", StructType([
        StructField("airport", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("gate", StringType(), True),
        StructField("baggage", StringType(), True),
        StructField("delay", StringType(), True),
        StructField("scheduled", StringType(), True),
        StructField("estimated", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("estimated_runway", StringType(), True),
        StructField("actual_runway", StringType(), True)
    ])),
    StructField("airline", StructType([
        StructField("name", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True)
    ])),
    StructField("flight", StructType([
        StructField("number", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("codeshared", StringType(), True)
    ])),
    StructField("aircraft", StringType(), True),
    StructField("live", StringType(), True)
])

col_map = {"flight_date": "flight_date",
           "flight_status": "flight_status",
           "departure.airport": "departure_airport",
           "departure.iata": "departure_iata",
           "departure.icao": "departure_icao",
           "departure.scheduled": "departure_scheduled",
           "departure.estimated": "departure_estimated",

           "arrival.airport": "arrival_airport",
           "arrival.iata": "arrival_iata",
           "arrival.icao": "arrival_icao",
           "arrival.scheduled": "arrival_scheduled",
           "arrival.estimated": "arrival_estimated",

           "airline.name": "airline_name",
           "airline.iata": "airline_iata",
           "airline.icao": "airline_icao",

           "flight.number": "flight_number",
           "flight.iata": "flight_iata",
           "flight.icao": "flight_icao",
           }

# Auxiliary functions


def convert_to_date(epoch): return datetime.utcfromtimestamp(
    epoch).strftime('%Y-%m-%d')


def convert_to_hour(epoch, start): return int(
    (int(epoch) - int(start))/3600) + 1


def log_message(message, level="info"):
    log_string = "{} [{}] {}".format(datetime.utcfromtimestamp(
        time.time()).strftime('%Y-%m-%d %H:%M:%S'), level, message)
    with open(log_file_path, "a") as file:
        file.write(log_string)
        file.close()

    if DEBUG:
        print(message)

# Function to download external


def download_files():
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/"

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for filename in files:
        try:
            response = requests.get(url + filename)
            response.raise_for_status()

            filepath = os.path.join(folder_path, filename)
            with open(filepath, 'wb') as file:
                file.write(response.content)

            print("Download successful. File saved to: {}".format(filepath))

        except requests.exceptions.RequestException as e:
            print("Download error: {}".format(e))


download_files()

# Read Airport Data
column_names = ["Airport ID", "Name of airport", "City", "Country", "IATA", "ICAO", "Latitude",
                "Longitude", "Altitude", "Timezone", "DST", "Tz database timezone", "Type", "Source"]
airport_df = pd.read_csv(folder_path + "/" + "airports.dat",
                         delimiter=',', names=column_names)
airport_df = airport_df.drop(columns=["Type", "Source"])

# Setup the Open-Meteo API client with cache and retry on error
# cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
# retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
# openmeteo = openmeteo_requests.Client(session = retry_session)
openmeteo = openmeteo_requests.Client()

url = "https://api.open-meteo.com/v1/gfs"

# Function to get risk percentile for each coordinate on flight path


def get_percentile(element, flight_start_date, flight_end_date, past_days=90):
    global COUNTS
    COUNTS = COUNTS + 1

    lat = element["lat"]
    lon = element["lon"]
    epoch = element["time"]

    a = flight_start_date
    b = flight_end_date
    flight_start_date = min(a, b)
    flight_end_date = max(a, b)

    start = convert_to_date(flight_start_date)
    end = convert_to_date(flight_end_date)
    hour = convert_to_hour(epoch, flight_start_date)

    percentiles_list = []
    location = {"latitude": lat, "longitude": lon}

    # historical data pull
    end_date = start
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") -
                  timedelta(days=past_days)).strftime("%Y-%m-%d")

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["cloud_cover", "cloud_cover_high", "precipitation", "snow_depth", "visibility", "weather_code", "lifted_index"]
    }
    params["latitude"] = location["latitude"]
    params["longitude"] = location["longitude"]
    hist_responses = openmeteo.weather_api(url, params=params)

    hist_response = hist_responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hist_hourly = hist_response.Hourly()
    cloud_cover = hist_hourly.Variables(0).ValuesAsNumpy()
    cloud_cover_high = hist_hourly.Variables(1).ValuesAsNumpy()
    precipitation = hist_hourly.Variables(2).ValuesAsNumpy()
    snow_depth = hist_hourly.Variables(3).ValuesAsNumpy()
    visibility = hist_hourly.Variables(4).ValuesAsNumpy()
    weather_code = hist_hourly.Variables(5).ValuesAsNumpy()
    lifted_index = hist_hourly.Variables(6).ValuesAsNumpy()

    hist_hourly_data = {}
    hist_hourly_data["cloud_cover"] = cloud_cover
    hist_hourly_data["cloud_cover_high"] = cloud_cover_high
    hist_hourly_data["precipitation"] = precipitation
    hist_hourly_data["snow_depth"] = snow_depth
    hist_hourly_data["visibility"] = visibility
    hist_hourly_data["weather_code"] = weather_code
    hist_hourly_data["lifted_index"] = lifted_index

    hist_hourly_data = pd.DataFrame(data=hist_hourly_data)
    hist_hourly_data["latitude"] = lat
    hist_hourly_data["longitude"] = lon

    # forecast pull
    assert hour > 0  # hour has to be greater than 0

    params = {
        "start_date": start,
        "end_date": end,
        "hourly": ["cloud_cover", "cloud_cover_high",
                   "precipitation", "snow_depth",
                   "visibility", "weather_code",
                   "lifted_index"]}
    params["latitude"] = location["latitude"]
    params["longitude"] = location["longitude"]
    for_responses = openmeteo.weather_api(url, params=params)

    for_response = for_responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    for_hourly = for_response.Hourly()
    cloud_cover = for_hourly.Variables(0).ValuesAsNumpy()
    cloud_cover_high = for_hourly.Variables(1).ValuesAsNumpy()
    precipitation = for_hourly.Variables(2).ValuesAsNumpy()
    snow_depth = for_hourly.Variables(3).ValuesAsNumpy()
    visibility = for_hourly.Variables(4).ValuesAsNumpy()
    weather_code = for_hourly.Variables(5).ValuesAsNumpy()
    lifted_index = for_hourly.Variables(6).ValuesAsNumpy()

    for_hourly_data = {}
    for_hourly_data["cloud_cover_forecast"] = cloud_cover
    for_hourly_data["cloud_cover_high_forecast"] = cloud_cover_high
    for_hourly_data["precipitation_forecast"] = precipitation
    for_hourly_data["snow_depth_forecast"] = snow_depth
    for_hourly_data["visibility_forecast"] = visibility
    for_hourly_data["weather_code_forecast"] = weather_code
    for_hourly_data["lifted_index_forecast"] = lifted_index

    for_hourly_dataframe = pd.DataFrame(data=for_hourly_data)
    for_hourly_dataframe["latitude"] = location["latitude"]
    for_hourly_dataframe["longitude"] = location["longitude"]
    for_hourly_dataframe['hour'] = for_hourly_dataframe.reset_index().index

    forecast_df = for_hourly_dataframe.loc[for_hourly_dataframe["hour"] == hour - 1]

    # percentiles
    for i in ["precipitation", "visibility", "snow_depth"]:
        for col in forecast_df.columns:
            if i in col:
                percentiles_list.append(stats.percentileofscore(
                    hist_hourly_data.loc[:, i], forecast_df[col].to_numpy()))

    percentiles_list = [np.nan_to_num(arr, nan=-1.0)
                        for arr in percentiles_list]
    time.sleep(1)
    return {"val": round(np.max(percentiles_list), 1)}


def add_weather_data(ele, start_date, end_date):
    return json.dumps(list(map(lambda x: get_percentile(x, int(start_date), int(end_date)), ele)))

# Functions to get path along flight path


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(
        radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return str(round(distance, 4))


def haversine_lambda(lat1, lon1, lat2, lon2): return 6371 * 2 * atan2(sqrt(sin(radians(lat2 - lat1) / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(
    radians(lon2 - lon1) / 2) ** 2), sqrt(1 - (sin(radians(lat2 - lat1) / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(radians(lon2 - lon1) / 2) ** 2)))


def get_points_along_flight_path(start_coord, end_coord, num_points, start_time, end_time):
    start_lat, start_lon = math.radians(
        start_coord[0]), math.radians(start_coord[1])
    end_lat, end_lon = math.radians(end_coord[0]), math.radians(end_coord[1])

    # points = []
    # for i in range(num_points + 1):
    #     fraction = i / num_points
    #     intermediate_lat = start_lat + fraction * (end_lat - start_lat)
    #     intermediate_lon = start_lon + fraction * (end_lon - start_lon)
    #     intermediate_time = start_time + fraction * (end_time - start_time)
    #     intermediate_point = {"lat": round(math.degrees(intermediate_lat), 5), "lon": round(math.degrees(intermediate_lon), 5), "time": int(intermediate_time)}
    #     points.append(intermediate_point)
    num_points = 3
    fractions = np.linspace(0, 1, num_points + 1)

    intermediate_lat = start_lat + fractions * (end_lat - start_lat)
    intermediate_lon = start_lon + fractions * (end_lon - start_lon)
    intermediate_time = start_time + fractions * (end_time - start_time)

    intermediate_lat_deg = np.degrees(intermediate_lat)
    intermediate_lon_deg = np.degrees(intermediate_lon)
    intermediate_time_int = (intermediate_time + 0.5).astype(int)

    points = list(map(lambda lat, lon, time: {"lat": round(lat, 5), "lon": round(lon, 5), "time": int(time)},
                      intermediate_lat_deg, intermediate_lon_deg, intermediate_time_int))

    return points


def get_airport_lat(airport_iata):
    selected_values = airport_df.loc[airport_df['IATA']
                                     == airport_iata, 'Latitude'].values.tolist()
    return str(round(selected_values[0], 4)) if len(selected_values) == 1 else "NaN"


def get_airport_lon(airport_iata):
    selected_values = airport_df.loc[airport_df['IATA']
                                     == airport_iata, 'Longitude'].values.tolist()
    return str(round(selected_values[0], 4)) if len(selected_values) == 1 else "NaN"


def trace_flight_path(lat1, lon1, lat2, lon2, distance, start_time, end_time):
    num_points = min(max(int(float(distance)//DISTANCE_THRESHOLD), 1), 30)
    return json.dumps(get_points_along_flight_path((float(lat1), float(lon1)), (float(lat2), float(lon2)), num_points, int(start_time), int(end_time)))


def get_timestamp(datetime_str):
    datetime_obj = datetime.fromisoformat(datetime_str)
    if datetime_obj.tzinfo is not None:
        datetime_obj_utc = datetime_obj.astimezone(pytz.utc)
    else:
        datetime_obj_utc = pytz.utc.localize(datetime_obj)
    timestamp = datetime.timestamp(datetime_obj_utc)

    return str(int(timestamp))


# UDF funcs for Pyspark
airport_lat_UDF = udf(lambda x: get_airport_lat(x), StringType())
airport_lon_UDF = udf(lambda x: get_airport_lon(x), StringType())
airport_dist_UDF = udf(
    lambda w, x, y, z: haversine_distance(w, x, y, z), StringType())
flight_path_UDF = udf(lambda t, u, v, w, x, y, z: trace_flight_path(
    t, u, v, w, x, y, z), StringType())
flight_DT_timestamp_UDF = udf(lambda x: get_timestamp(str(x)), StringType())
weather_UDF = udf(lambda x, y, z: add_weather_data(json.loads(x), y, z))

# Function to get flights from API


def get_flights(api):
    data = None
    try:
        url = "http://api.aviationstack.com/v1/flights?access_key={}&flight_Status=scheduled&limit=100".format(
            api)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
        else:
            log_message("Request failed with status code: {}".format(
                response.status_code), level="ERR")

    except requests.exceptions.RequestException as e:
        log_message("Request error: {}".format(e), level="ERR")

    return data


def remove_whitespace(obj):
    if isinstance(obj, str):
        # return obj.replace(" ", "").replace("\t", "")
        return ' '.join(part.strip() for part in obj.split())
    elif isinstance(obj, list):
        return [remove_whitespace(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: remove_whitespace(value) for key, value in obj.items()}
    else:
        return obj

# Function to extract data from flight data variable


def extract_data(test=True, debug=True):
    global flights_data
    for _ in range(MAX_TRIES):
        if test:
            flights_data = get_flights(API_KEY)
            if flights_data is None:
                log_message("No data macha!", level="ERR")
                return None

            if debug:
                print("Data before removing whitespace")
                print(flights_data)
                print("\n")

            flights_data = remove_whitespace(flights_data)

            if len(flights_data["data"]) == 0:
                with open(os.path.join(file_path, "aviation_stack_jsondata.json"), "r") as file:
                    flights_data = json.load(file)
                    file.close()

            else:
                with open(os.path.join(file_path, "aviation_stack_jsondata_updated.json"), 'w') as file:
                    json.dump(flights_data, file, separators=(',', ':'))

                # break
                return flights_data

            if debug:
                print("Data after removing whitespace")
                print(flights_data)
                print(json.dumps(flights_data["data"][0], indent=2))

        else:
            with open(os.path.join(file_path, "aviation_stack_jsondata.json"), "r") as file:
                flights_data = json.load(file)
                file.close()

                # break
                return flights_data

# Function for first application
# exports the schedule to flights.csv


def app1():
    flights_data = extract_data(DEPLOYED, DEBUG)
    while not flights_data:
        flights_data = extract_data(DEPLOYED, DEBUG)
    pagination = flights_data["pagination"]
    print(pagination)
    data = flights_data["data"]

    df = spark.createDataFrame(data, schema=json_schema)
    # Run this if you're running into rate limit issues
    df = df.limit(20)

    df = df.select(*[col(k).alias(col_map[k]) for k in col_map])

    # applying UDFs to the df
    df = df.dropna(subset=['flight_status'])
    df = df.withColumn("departure_lat", airport_lat_UDF("departure_iata"))
    df = df.withColumn("departure_lon", airport_lon_UDF("departure_iata"))
    df = df.withColumn("arrival_lat", airport_lat_UDF("arrival_iata"))
    df = df.withColumn("arrival_lon", airport_lon_UDF("arrival_iata"))

    df = df.filter(~(df["departure_lat"] == "NaN"))
    df = df.filter(~(df["departure_lon"] == "NaN"))
    df = df.filter(~(df["arrival_lat"] == "NaN"))
    df = df.filter(~(df["arrival_lon"] == "NaN"))

    df = df.withColumn("departure_epoch",
                       flight_DT_timestamp_UDF("departure_scheduled"))
    df = df.withColumn(
        "arrival_epoch", flight_DT_timestamp_UDF("arrival_scheduled"))
    df = df.withColumn("airport_distance", airport_dist_UDF(
        "departure_lat", "departure_lon", "arrival_lat", "arrival_lon"))
    df = df.withColumn("flight_path", flight_path_UDF("departure_lat", "departure_lon",
                       "arrival_lat", "arrival_lon", "airport_distance", "departure_epoch", "arrival_epoch"))
    df = df.withColumn("weather_data", weather_UDF(
        "flight_path", "departure_epoch", "arrival_epoch"))
    df.toPandas().to_csv(os.path.join(file_path, "flights.csv"))

# ==================================================================#
# Individual Flight Tracker
# ==================================================================#

# Auxliary functions for individual flight tracker


def get_col_name(x): return x.replace(".", "_")


def cleanup_df(df, column):
    previous_count = df.count()
    df = df.filter(df[column] != "NaN")
    df = df.na.drop(subset=[column])
    next_count = df.count()
    print("Before - {}, after - {}, difference - {}, %loss - {}".format(previous_count,
          next_count, previous_count - next_count, (previous_count - next_count)*100.0/previous_count))
    return df


def check_valid_code(airport_iata):
    selected_values = airport_df.loc[airport_df['IATA']
                                     == airport_iata, 'Latitude'].values.tolist()
    return True if len(selected_values) == 1 else False


def get_flights_from_airport(airport_iata):
    if check_valid_code(airport_iata) is False:
        return None

    airport_details = flight_api.get_airport_details(code=airport_iata)
    keys = ["flight.identification.id", "flight.identification.callsign", "flight.airline.name", "flight.airport.origin",
            "flight.airport.destination", "flight.time.scheduled", "flight.time.real", "flight.time.estimated"]

    arr_details = airport_details["airport"]["pluginData"]["schedule"]["arrivals"]["data"]

    arr_df = spark.createDataFrame(arr_details)
    arr_df = arr_df.withColumn("Flight_type", lit("arrivals"))

    for key in keys:
        arr_df = arr_df.withColumn(key.replace(".", "_"), col(key))

    dept_details = airport_details["airport"]["pluginData"]["schedule"]["departures"]["data"]

    dept_df = spark.createDataFrame(dept_details)
    dept_df = dept_df.withColumn("Flight_type", lit("departures"))

    for key in keys:
        dept_df = dept_df.withColumn(key.replace(".", "_"), col(key))
    df = dept_df.union(arr_df)
    df = df.toPandas()
    return df

# Function to get individual flight detail for each flight id


def get_flight_details(flight_id):
    details = flight_api.get_flight_details(flight_id)

    data = {}
    data["id"] = details["identification"]["id"]
    data["callsign"] = details["identification"]["callsign"]

    data["aircraft_modelname"] = details["aircraft"]["model"]["text"]
    data["aircraft_modelcode"] = details["aircraft"]["model"]["code"]

    data["airline_name"] = details["airline"]["name"]

    data["origin_airport_name"] = details["airport"]["origin"]["name"]
    data["origin_airport_iata"] = details["airport"]["origin"]["code"]["iata"]
    data["origin_airport_lat"] = details["airport"]["origin"]["position"]["latitude"]
    data["origin_airport_lon"] = details["airport"]["origin"]["position"]["longitude"]
    data["origin_airport_alt"] = details["airport"]["origin"]["position"]["altitude"]
    data["origin_airport_country"] = details["airport"]["origin"]["position"]["country"]["name"]

    data["destination_airport_name"] = details["airport"]["destination"]["name"]
    data["destination_airport_iata"] = details["airport"]["destination"]["code"]["iata"]
    data["destination_airport_lat"] = details["airport"]["destination"]["position"]["latitude"]
    data["destination_airport_lon"] = details["airport"]["destination"]["position"]["longitude"]
    data["destination_airport_alt"] = details["airport"]["destination"]["position"]["altitude"]
    data["destination_airport_country"] = details["airport"]["destination"]["position"]["country"]["name"]

    data["scheduled_departure_time"] = details["time"]["scheduled"]["departure"]
    data["scheduled_arrival_time"] = details["time"]["scheduled"]["arrival"]
    data["real_departure_time"] = details["time"]["real"]["departure"]
    data["real_arrival_time"] = details["time"]["real"]["arrival"]
    data["estimated_departure_time"] = details["time"]["estimated"]["departure"]
    data["estimated_arrival_time"] = details["time"]["estimated"]["arrival"]

    data["historical_flighttime"] = details["time"]["historical"]["flighttime"]
    data["historical_delay"] = details["time"]["historical"]["delay"]

    curr_flight_schema = StructType([
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True),
        StructField("alt", IntegerType(), True),
        StructField("spd", IntegerType(), True),
        StructField("ts", IntegerType(), True),
        StructField("hd", IntegerType(), True)
    ])

    df = spark.createDataFrame(details["trail"], schema=curr_flight_schema)
    df = df.withColumnRenamed("ts", "time")
    sampling_interval = 10

    df = df.rdd.zipWithIndex().filter(
        lambda x: x[1] % sampling_interval == 0).map(lambda x: x[0]).toDF()
    # df.show(truncate=False)
    data["trail"] = df.toJSON().collect()

    latest_data = details["trail"][0]
    data["future_path"] = get_points_along_flight_path((latest_data["lat"], latest_data["lng"]),
                                                       (data["destination_airport_lat"],
                                                        data["destination_airport_lon"]),
                                                       10,
                                                       latest_data["ts"], data["scheduled_arrival_time"])

    return data


# App definition
app = Dash(external_stylesheets=[dbc.themes.DARKLY])
load_figure_template('DARKLY')

# Styling options for tabs
tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold',
    'backgroundColor': '#222222'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

# Layout of Application
app.layout = html.Div([
    # Each application is in a separate tab
    dcc.Tabs([
        # First application
        dcc.Tab(label="Future Flight Planner", children=[
            html.H1(children='Flights Map', style={'textAlign': 'center'}),
            # Plot globe plot of all flights with risk
            dcc.Graph(id='live-update-map',
                      style={'height': '100vh', 'width': '100vw'}),
            # Scheduler to run every 12 hours
            dcc.Interval(id='interval-component',
                         interval=12*60*60*1000, n_intervals=0)
        ], style=tab_style, selected_style=tab_selected_style),
        # Second Application
        dcc.Tab(label="Individual Flight Tracker", children=[
            html.H1(children="Individual Flight Tracker",
                    style={'textAlign': 'center'}),
            html.Br(),
            # Select airport
            dcc.Dropdown(airport_options, id='airport_dropdown',
                         className='darkly-dropdown', style={'background-color': 'blue'}),
            html.Br(),
            # Display arrivals
            dash_table.DataTable(id='flight_table', columns=[], data=[], page_action='none', style_table={
                                 'height': '300px', 'overflowY': 'auto'}, style_cell={'background-color': '#222222', 'font-family': 'monospace', 'textAlign': 'center'}),
            html.Br(),
            # Select flight
            dcc.Dropdown(id='flight_dropdown', className='darkly-dropdown'),
            # Plot flight route
            dcc.Graph(id='flight_output', style={'width': '100vw'})
        ], style=tab_style, selected_style=tab_selected_style)
    ], style=tabs_styles)
])

# Callback for future flight planner


@app.callback(
    Output('live-update-map', 'figure'),
    Input('interval-component', 'n_intervals')
)
def format_csv_and_plot(n):
    # Get csv of flight schedules
    app1()

    df = pd.read_csv("flights.csv")
    # Format Columns
    df['flight_path'] = df['flight_path'].map(lambda x: eval(x))
    df['flight_path'] = df['flight_path'].map(
        lambda x: [(j['lat'], j['lon']) for j in x])
    df['weather_data'] = df['weather_data'].map(lambda x: eval(x))
    df['weather_data'] = df['weather_data'].map(
        lambda x: [j['val'] for j in x])
    # Color by risk threshold
    df['weather_data'] = df['weather_data'].map(
        lambda x: ["red" if j >= 90 else ("orange" if j >= 75 else "white") for j in x])

    # Get Airports
    dept_airport_names = df.groupby('departure_airport')[
        'departure_lat'].first().index.tolist()
    dept_aiport_lat = df.groupby('departure_airport')[
        'departure_lat'].first().tolist()
    dept_aiport_lon = df.groupby('departure_airport')[
        'departure_lon'].first().tolist()

    # Arrival Airports
    arr_airport_names = df.groupby('arrival_airport')[
        'arrival_lat'].first().index.tolist()
    arr_aiport_lat = df.groupby('arrival_airport')[
        'arrival_lat'].first().tolist()
    arr_aiport_lon = df.groupby('arrival_airport')[
        'arrival_lon'].first().tolist()

    flight_details = df[['flight_path', 'weather_data',
                         'flight_iata', 'arrival_epoch', 'departure_epoch']]
    # Plot Globe plot of all flights
    fig = go.Figure(go.Scattergeo())
    for index, row in flight_details.iterrows():
        path = row['flight_path']
        risk_colors = row['weather_data']
        flight_number = row['flight_iata']
        arrival_time = datetime.fromtimestamp(
            int(row['arrival_epoch'])).strftime("%Y-%m-%d %H:%M:%S")
        departure_time = datetime.fromtimestamp(
            int(row['departure_epoch'])).strftime("%Y-%m-%d %H:%M:%S")
        txt = f"Flight: {flight_number}<br>Departure Time: {departure_time}<br>Arrival Time {arrival_time}"
        fig.add_trace(go.Scattergeo(lat=[p[0] for p in path],
                                    lon=[p[1] for p in path],
                                    mode='markers + lines',
                                    showlegend=False,
                                    line=dict(color='white', width=0.5),
                                    marker=dict(color=risk_colors, size=7),
                                    text=txt,
                                    hoverinfo='text'
                                    )
                      )
    fig.add_trace(go.Scattergeo(lat=dept_aiport_lat,
                                lon=dept_aiport_lon,
                                showlegend=False,
                                marker=dict(
                                    color="#ba1fbf", symbol='square', size=7),
                                hoverinfo="text",
                                text=dept_airport_names))

    fig.add_trace(go.Scattergeo(lat=arr_aiport_lat,
                                lon=arr_aiport_lon,
                                showlegend=False,
                                marker=dict(
                                    color="#ba1fbf", symbol='square', size=7),
                                hoverinfo="text",
                                text=arr_airport_names))

    fig.update_layout(geo_showland=True,
                      geo_showsubunits=True,
                      geo_landcolor="#45bf1f",
                      geo_showcountries=True,
                      geo_countrycolor='Black',
                      geo_showocean=True,
                      geo_oceancolor="DarkBlue",
                      geo_projection_type="orthographic")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig

# Get table of arrivals and dropdown of flights from dropdown of airports


@app.callback(
    Output('flight_table', 'columns'),
    Output('flight_table', 'data'),
    Output('flight_dropdown', 'options'),
    Input('airport_dropdown', 'value')
)
def create_flight_schedule(value):
    # When the user has not made a choice yet
    if not value:
        return [], [], ["No Flights Available"]

    # Fetch fliights
    df = get_flights_from_airport(value)
    df = df.drop('flight', axis=1)

    # Format spark dataframe for compatibility
    result_df = {'Flight ID': [], 'Flight Number': [], 'Airline': [],
                 'Origin': [], 'Departure Time': [], 'Arrival Time': []}
    for index, row in df.iterrows():
        if not row['flight_identification_id']:
            continue
        if row['Flight_type'] == 'departures':
            continue
        result_df['Flight ID'].append(row['flight_identification_id'])
        result_df['Flight Number'].append(
            row['flight_identification_callsign'])
        result_df['Airline'].append(row['flight_airline_name'])
        orig = row['flight_airport_origin']
        origin_airport_name = re.search(r'^(.+?),', orig).group()[6:-1]
        result_df['Origin'].append(origin_airport_name)
        times = row['flight_time_scheduled']
        # Due to some subdictionaries actually being strings
        # Regex is required to extract relevant info
        arrival_time = re.search(
            r'\d+', re.search(r'a(.+?),', times).group()).group()
        departure_time = re.search(
            r'\d+', re.search(r'd(.+?)}', times).group()).group()
        result_df['Arrival Time'].append(datetime.fromtimestamp(
            int(arrival_time)).strftime("%Y-%m-%d %H:%M:%S"))
        result_df['Departure Time'].append(datetime.fromtimestamp(
            int(departure_time)).strftime("%Y-%m-%d %H:%M:%S"))

    result_df = pd.DataFrame(result_df)
    columns = [{'name': col, 'id': col} for col in result_df.columns]
    data = result_df.to_dict('records')
    flights = []
    flight_map = result_df[['Flight ID', 'Flight Number']]
    flight_mapper = {}
    # Compatibility with dash
    for index, row in flight_map.iterrows():
        flight_mapper[row['Flight Number']] = row['Flight ID']
    flights = [{'label': key, 'value': value}
               for key, value in flight_mapper.items()]
    return columns, data, flights

# Plot individual flight after selection


@app.callback(
    Output('flight_output', 'figure'),
    Input('flight_dropdown', 'value')
)
def plot_graph(value):
    if not value:
        return go.Figure(go.Scattergeo())
    # Fetch flight
    flight = get_flight_details(value)
    # Extract departure airport
    start_pt = (flight['origin_airport_lat'], flight['origin_airport_lon'])
    # Arrival Airport
    end_pt = (flight['destination_airport_lat'],
              flight['destination_airport_lon'])
    trail = flight['trail']
    # Due to stringificaiton of subdirectories, evaluate strings literally
    prev_path = [(eval(l)['lat'], eval(l)['lng']) for l in trail]
    cur_location = prev_path[0]
    f_path = flight['future_path']
    future_path = [(fut['lat'], fut['lon']) for fut in f_path]
    # Plot Inidivudal points and paths
    fig = go.Figure(go.Scattergeo(
        lat=[p[0] for p in prev_path],
        lon=[p[1] for p in prev_path],
        mode='markers+lines',
        line=dict(color='white', width=0.8),
        marker=dict(color='white', size=7),
        showlegend=False,
        hoverinfo="text",
        text="Previous Path"
    ))

    fig.add_trace(go.Scattergeo(
        lat=[f[0] for f in future_path],
        lon=[f[1] for f in future_path],
        mode='markers+lines',
        line=dict(color='LightBlue', width=0.8),
        marker=dict(color='LightBlue', size=7),
        showlegend=False,
        hoverinfo="text",
        text="Future Path"
    ))

    fig.add_trace(go.Scattergeo(
        lat=[start_pt[0]],
        lon=[start_pt[1]],
        mode='markers',
        marker=dict(color='pink', symbol='square', size=10),
        showlegend=False,
        hoverinfo="text",
        hovertext=flight['origin_airport_name']
    ))

    fig.add_trace(go.Scattergeo(
        lat=[end_pt[0]],
        lon=[end_pt[1]],
        mode='markers',
        marker=dict(color='pink', symbol='square', size=10),
        showlegend=False,
        hoverinfo="text",
        text=flight['destination_airport_name']
    ))

    fig.add_trace(go.Scattergeo(
        lat=[cur_location[0]],
        lon=[cur_location[1]],
        mode='markers',
        marker=dict(color='red', size=10),
        showlegend=False,
        hoverinfo='text',
        text="Current Location"
    ))

    fig.update_layout(autosize=True, geo_center=dict(
        lat=cur_location[0], lon=cur_location[1]), geo_projection_scale=25, geo_showland=True,
        geo_showsubunits=True,
        geo_subunitcolor='Black',
        geo_landcolor="#45bf1f",
        geo_showcountries=True,
        geo_countrycolor='Black',
        geo_showocean=True,
        geo_oceancolor="DarkBlue")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig


if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8050, debug=False)
    # App will run on http://0.0.0.0:8050 on local system
