import json
import logging
import boto3
import redis
from decimal import Decimal
from datetime import datetime

from tentrr import tentrr_lambda
from tentrr.message import sns_publish
from dynamo import StateParkSitesDB, T4SiteDetails

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()

REDIS_HOST = "tentrrlambda.kwuyrj.ng.0001.use1.cache.amazonaws.com"
cache = redis.Redis(host=REDIS_HOST, port=6379, db=0)

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

class TentrrEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 != 0:
                return float(o)
            else:
                return int(o)
        return json.JSONEncoder.default(self, o)

def deserialize_dynamo(record):
    return {k: deserializer.deserialize(v) for k, v in record.items()}

def write_to_cache(resource, identifier, body):
    redis_key = "_".join([resource, identifier])

    logger.info(f"Setting {redis_key}")
    cache.set(redis_key, json.dumps(body))


def read_from_cache(resource, identifier):
    redis_key = "_".join([resource, identifier])

    logger.info(f"Fetching {redis_key} from Redis")
    response = cache.get(redis_key)

    if response:
        data = response.decode("utf-8")
        logger.info("Retrieved from cache: ")
        logger.info(data)
    else:
        data = None
        logger.info("No state park cache session found")

    return data

def publish_event(state, payload, config):
    ''' Publish SNS event

        Parmeters:
        payload (dict)
        config (dict)
    '''
    payload = json.dumps(payload)
    event_type = f'statepark.availability.{state}'
    topic_arn = config.get('MESSAGE_SNS_TOPIC_ARN')
    sns_publish(topic_arn, 
        event_type, 
        payload, 
        compress=False)

def handle_event(record, config):
    if record['dynamodb'].get('NewImage'):
        new_data = deserialize_dynamo(record['dynamodb']['NewImage'])
    else:
        logger.info("Only processing events with new images, returning...")
        return default_response

    if new_data["source"] in ["state_parks"]:
        logger.info("Not processing reservations from source: {}".format(new_data["source"]))
        return default_response

    status = new_data["status"]
    if status not in ["confirmed", "canceled"]:
        logger.info("Not processing reservations with status: {}".format(status))
        return default_response
        
    logger.info("New data: ")
    logger.info(json.dumps(new_data, cls=TentrrEncoder))

    details_ddb = T4SiteDetails(config)
    listing_id = new_data.get("listingId")
    if not listing_id:
        logger.info("Unable to find listing id. returning...")
        return default_response
        
    site_details = details_ddb.get_site_details_by_listing(listing_id)

    if site_details is None:
        logger.error(f"Site not found with listing {listing_id}")
        return default_response

    if site_details.get("badge_type_display") != "State Park Site":
        logger.info("Only processing state park reservations, returning...")
        return default_response

    today = datetime.utcnow()
    start_datetime = datetime.strptime(new_data["checkInDateLocalized"], "%Y-%m-%d")

    if today > start_datetime:
        logger.info("Only blocking state park reservations in the future, returning...")
        return default_response

    state = site_details["state_name"].lower()

    if state not in ["utah"]:
        logger.info(f"Not processing SP updates in this state: {state}")
        return default_response

    reservation_id = new_data["id"]
    park = site_details.get("park")
    ra_name = site_details.get("reserve_america_name")
    ra_format = site_details.get("reserve_america_format")

    if not park or not ra_name or not ra_format:
        logger.error(f"State Park with missing details for reservation: {reservation_id}")
        return default_response

    start_date = datetime.strftime(datetime.strptime(new_data["checkInDateLocalized"], "%Y-%m-%d"), "%a %b %d %Y")
    end_date = datetime.strftime(datetime.strptime(new_data["checkOutDateLocalized"], "%Y-%m-%d"), "%a %b %d %Y")

    payload = {
        "event": status,
        "reservation_id": reservation_id,
        "park": park,
        "ra_name": ra_name,
        "format": ra_format,
        "start": start_date,
        "end": end_date
    }

    cache = read_from_cache(f"state_parks_{status}", reservation_id)
    if not cache:
        write_to_cache(f"state_parks_{status}", reservation_id, payload)
        publish_event(state, payload, config)
        return default_response
    else:
        return default_response

@tentrr_lambda
def lambda_handler(event, context, config=None):

    logger.info("Processing {} records:".format(len(event["Records"])))
    for record in event.get("Records"):
        response = handle_event(record, config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }

    return response