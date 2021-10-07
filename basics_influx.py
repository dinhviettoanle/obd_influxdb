# Documentation : https://docs.influxdata.com/influxdb/v2.0/api-guide/client-libraries/python/
# Et ca : https://influxdb-client.readthedocs.io/en/stable/
# Peut etre utile : https://github.com/fabio-miranda/csv-to-influxdb

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions

import rx
from rx import operators as ops

import argparse
from collections import OrderedDict
from csv import DictReader



# Init client
bucket = "mybucket"
org = "obd_influxdb"
token = None
url="http://localhost:8086"





def write_db():
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # TODO : Gérer le timestamp
    p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3) 
    write_api.write(bucket=bucket, org=org, record=p)
    print("Done !")




def query_db():
    query_api = client.query_api()
    query = f"""from(bucket:"{bucket}")\
    |> range(start: -10m)\
    |> filter(fn:(r) => r._measurement == "my_measurement")\
    |> filter(fn: (r) => r.location == "Prague")\
    |> filter(fn:(r) => r._field == "temperature" )"""

    result = query_api.query(org=org, query=query)
    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_field(), record.get_value()))

    print(results)



def delete_db():
    delete_api = client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2022-02-01T00:00:00Z"
    delete_api.delete(start, stop, '_measurement="my_measurement"', bucket=bucket, org=org)
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

    """
        For better performance is sometimes useful directly create a LineProtocol to avoid unnecessary escaping overhead:
        """
        # from pytz import UTC
        # import ciso8601
        # from influxdb_client.client.write.point import EPOCH
        #
        # time = (UTC.localize(ciso8601.parse_datetime(row["Date"])) - EPOCH).total_seconds() * 1e9
        # return f"financial-analysis,type=vix-daily" \
        #        f" close={float(row['VIX Close'])},high={float(row['VIX High'])},low={float(row['VIX Low'])},open={float(row['VIX Open'])} " \
        #        f" {int(time)}"

    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(row['VIX Open'])) \
        .field("high", float(row['VIX High'])) \
        .field("low", float(row['VIX Low'])) \
        .field("close", float(row['VIX Close'])) \
        .time(row['Date'])




def write_csv():
    # From https://influxdb-client.readthedocs.io/en/stable/usage.html#how-to-efficiently-import-large-dataset

    data = rx \
        .from_iterable(DictReader(open('data/vix-daily.csv', 'r'))) \
        .pipe(ops.map(lambda row: parse_row(row)))

    write_api = client.write_api(write_options=WriteOptions(batch_size=50_000, flush_interval=10_000))
    write_api.write(bucket=bucket, org=org, record=data)
    write_api.close()
    print("Done !")

    # Pour afficher dans le GUI : 
    #    from(bucket:"mybucket")
    #       |> range(start: 0, stop: now())



def test():
    return



if __name__ == '__main__':

    FUNCTION_MAP = {
        'test' : test,
        'write' : write_db,
        'query' : query_db,
        'delete' : delete_db,
        'write_csv' : write_csv,
        }

    parser = argparse.ArgumentParser()
    parser.add_argument('--token', required=True)
    parser.add_argument('--command', choices=FUNCTION_MAP.keys(), required=True)
    args = parser.parse_args()

    token = args.token
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org,
        timeout=60_000
    )

    func = FUNCTION_MAP[args.command]
    func()
    
    client.close()