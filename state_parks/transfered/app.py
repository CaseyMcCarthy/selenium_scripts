import json
import logging
import redis
import boto3

from tentrr.log import loghandler
from dynamo import RAReservationDB

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

REDIS_HOST = 'stateparks.kwuyrj.ng.0001.use1.cache.amazonaws.com'
    
def pull_cache():
    pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    data = {}
    cursor = '0'
    while cursor != 0:
        cursor, keys = client.scan(cursor=cursor)
        values = client.mget(keys)
        values = map(str, values)
        data.update(dict(zip(keys, values)))

    reservations = [val for val in data]
    return reservations
    
def handle_event(config):
    ra_ddb = RAReservationDB(config)
    
    redis_reservations = pull_cache()
    print("REDIS COUNT: ")
    print(len(redis_reservations))
    ddb_reservations = ra_ddb.pull_all_reservation_ids()
    print("DDB COUNT: ")
    print(len(ddb_reservations))
    
    missing_reservations = [res for res in redis_reservations + ddb_reservations if res in ddb_reservations and res not in redis_reservations]
    
    print("MISSING RESERVATIONS COUNT: ")
    print(len(missing_reservations))
    print(missing_reservations)

    for reservation_id in missing_reservations:
        ra_ddb.remove_reservation(reservation_id)
    
    return default_response

@loghandler
def lambda_handler(event, context):
    config = {
        "DYNAMODB_REGION": "us-east-1",
        "env": "prod"
    }

    response = handle_event(config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }

    return response
