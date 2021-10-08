# Documentation : https://docs.influxdata.com/influxdb/v2.0/api-guide/client-libraries/python/
# Et ca : https://influxdb-client.readthedocs.io/en/stable/
# Peut etre utile : https://github.com/fabio-miranda/csv-to-influxdb

import argparse

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

import rx
from rx import operators as ops
from collections import OrderedDict
from csv import DictReader
import pandas as pd
from tqdm.autonotebook import tqdm
from pprint import pprint


# ===== Init client =====

# Name of the bucket
bucket = "mybucket" 
# Name of the organization -> in the "profile picture" tab on the left, under the username
org = "" 
# Token of the user -> "data" tab on the left > tab "Tokens" > click on "{username}'s Token"
token = "" 
url="http://localhost:8086"

# =======================

datafile = 'data/vix-daily.csv'



def write_db():
    # Writes a single line in the database
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3) 
    write_api.write(bucket=bucket, org=org, record=p)
    print("Done !")




def query_db():
    # Queries the database 
    # required : financial-anaysis is already created

    query_api = client.query_api()
    query = f"""from(bucket:"{bucket}")\
    |> range(start: -2y)\
    |> filter(fn:(r) => r._measurement == "financial-analysis")\
    |> filter(fn:(r) => r._field == "open" )"""

    result = query_api.query(org=org, query=query)
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time().strftime("%d/%m/%Y, %H:%M:%S"), record.get_field(), record.get_value()))

    pprint(results)



def delete_db():
    # Removes the measurement "financial-analysis"

    delete_api = client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2022-02-01T00:00:00Z"
    delete_api.delete(start, stop, '_measurement="financial-analysis"', bucket=bucket, org=org)
    print("Done !")



def parse_row(row: OrderedDict):
    """Parse row of CSV file into Point with structure:

        financial-analysis,type=ily close=18.47,high=19.82,low=18.28,open=19.82 1198195200000000000

    CSV format:
        Date,VIX Open,VIX High,VIX Low,VIX Close\n
        2004-01-02,17.96,18.68,17.54,18.22\n
        2004-01-05,18.45,18.49,17.44,17.49\n
        2004-01-06,17.66,17.67,16.19,16.73\n
        2004-01-07,16.72,16.75,15.5,15.5\n
        2004-01-08,15.42,15.68,15.32,15.61\n
        2004-01-09,16.15,16.88,15.57,16.75\n
        ...

    :param row: the row of CSV file
    :return: Parsed csv row to [Point]
    """

    # Du coup, va renvoyer 4 points différents dans le _measurement "financial-analysis":
    # - {"type" : "vix-daily", "_field" : "open", "_value" : VIX_Open}
    # - {"type" : "vix-daily", "_field" : "high", "_value" : VIX_High}
    # - {"type" : "vix-daily", "_field" : "low", "_value" : VIX_Low}
    # - {"type" : "vix-daily", "_field" : "close", "_value" : VIX_Close}
    # D'où taille du dataset InfluxDB = 4*taille_du_csv, vu qu'on aura 4 tables
    
    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(row['VIX Open'])) \
        .field("high", float(row['VIX High'])) \
        .field("low", float(row['VIX Low'])) \
        .field("close", float(row['VIX Close'])) \
        .time(row['Date'])




def write_csv_async():
    # From https://influxdb-client.readthedocs.io/en/stable/usage.html#how-to-efficiently-import-large-dataset
    # Plus efficace, mais pas tout compris

    data = rx \
        .from_iterable(DictReader(open(datafile, 'r'))) \
        .pipe(ops.map(lambda row: parse_row(row)))

    write_api = client.write_api(write_options=WriteOptions(batch_size=50_000, flush_interval=10_000))
    write_api.write(bucket=bucket, org=org, record=data)
    write_api.close()
    print("Done !")




def write_csv_sync():
    # Writes in the database lines per lines

    # API init
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Data import as df
    df = pd.read_csv(datafile, sep=',')

    # Import loop on rows of df
    n_rows = len(df)
    pbar = tqdm(total=n_rows)
    for i, row in df.iterrows():
        p = parse_row(row)
        write_api.write(bucket=bucket, org=org, record=p)
        pbar.update(1)
    pbar.close()
    print("Done !")



def test():
    return



if __name__ == '__main__':

    FUNCTION_MAP = {
        'test' : test,
        'write' : write_db,
        'query' : query_db,
        'delete' : delete_db,
        'write_csv_async' : write_csv_async,
        'write_csv_sync' : write_csv_sync,
        }

    parser = argparse.ArgumentParser()
    parser.add_argument('--command', choices=FUNCTION_MAP.keys(), required=True)
    parser.add_argument('--token', required=False)
    parser.add_argument('--org', required=False)
    args = parser.parse_args()

    if args.token is not None and token == "":
        token = args.token
    if args.org is not None and org == "":
        org = args.org

    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org,
        timeout=60_000
    )

    func = FUNCTION_MAP[args.command]
    func()
    
    client.close()