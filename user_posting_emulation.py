import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import argparse


random.seed(100)


class AWSDBConnector:

    def __init__(self, host, user, password, database, port):
        self.HOST = host
        self.USER = user
        self.PASSWORD = password
        self.DATABASE = database
        self.PORT = port
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


def parse_arguments():
    parser = argparse.ArgumentParser(description="Database Connector Arguments")
    parser.add_argument("--host", type=str, help="Database host address")
    parser.add_argument("--user", type=str, help="Database user")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--database", type=str, help="Database name")
    parser.add_argument("--port", type=int, help="Database port")
    return parser.parse_args()


def run_infinite_post_data_loop(host, user, password, database, port):
    new_connector = AWSDBConnector(host, user, password, database, port)
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            

            invoke_url = "https://gldssox2ei.execute-api.us-east-1.amazonaws.com/test-api/topics/"
            Batch_name_pin = "0e4753f224a7-pin"
            Batch_name_geo = "0e4753f224a7-geo"
            Batch_name_user = "0e4753f224a7-user"

            payload_pin = json.dumps({
                "records": [
                    {
                        "value":pin_result

                    }
                ]
            })
            payload_geo = json.dumps({
                "records":[
                    {
                         "value":{"index": geo_result["ind"], "country": geo_result["country"], "timestamp": geo_result["timestamp"].isoformat(), 
                                  'latitude': geo_result["latitude"], "longitude": geo_result["longitude"]}
                    }
                ]
            })
            payload_user = json.dumps({
                "records":[
                    {
                          "value":{"index": user_result["ind"], "first_name": user_result["first_name"], 
                                   "last_name": user_result["last_name"], "age": user_result["age"], 
                                    "date_joined": user_result["date_joined"].isoformat()}
                    }
                ]
            })
            
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            response_pin = requests.request("PUT", f"{invoke_url}{Batch_name_pin}", headers=headers, data=payload_pin)
            response_geo = requests.request("PUT", f"{invoke_url}{Batch_name_geo}", headers=headers, data=payload_geo)
            response_user = requests.request("PUT", f"{invoke_url}{Batch_name_user}", headers=headers, data=payload_user)
            print(response_pin.json())
            print(response_geo.json())
            print(response_user.json())


def main():
    args = parse_arguments()
    host = args.host
    user = args.user
    password = args.password
    database = args.database
    port = args.port
    
    run_infinite_post_data_loop(host, user, password, database, port)
    print('Working')


if __name__ == "__main__":
    main()
