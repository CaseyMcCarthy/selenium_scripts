import json
import logging
import boto3
from datetime import datetime

from tentrr.log import loghandler
from dynamo import StateParkSitesDB

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

no_format = {
    "statusCode": 500,
    "body": json.dumps("No Login Format")
}

def deserialize_dynamo(record):
    return {k: deserializer.deserialize(v) for k, v in record.items()}

def format_data(update):
    return {
        "site_id": update["site_id"],
        "listing_id": update["guesty_listing_id"],
        "name": update["site_name"],
        "park": update["park"],
        "ra_name": update["reserve_america_name"],
        "state": update["state_name"],
        "format": update.get("reserve_america_format"),
        "last_updated": datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S")
    }

def insert_site(update, sp_ddb):
    if not update.get("park") or not update.get("reserve_america_name"):
        logger.info("Missing park or RA site name, returning...")
        return default_response

    data = format_data(update)
    res = sp_ddb.set_site(data)
    print(res)
    return default_response

def update_site(existing_site, update, sp_ddb):
    if not update.get("park") or not update.get("reserve_america_name"):
        logger.info("Missing park or RA site name, returnning...")
        return default_response

    if existing_site["state"] == "Utah" and not update.get("reserve_america_format"):
        logger.info("Utah state parks require a login format")
        return no_format

    update_park = update["park"]
    update_ra_name = update["reserve_america_name"]
    update_ra_format = update.get("reserve_america_format")

    existing_park = existing_site["park"]
    existing_ra_name = existing_site["ra_name"]
    existing_ra_format = existing_site.get("format")

    if ( 
        update_park != existing_park
        or update_ra_name != existing_ra_name
        or update_ra_format != existing_ra_format
    ):
        logger.info("State park info has changed.")

        data = format_data(update)
        res = sp_ddb.set_site(data)
        print(res)
    else:
        logger.info("No changes to site, returning....")

    return default_response

def handle_event(record, config):
    logger.info("Event type: {}".format(record["eventName"]))

    if record['dynamodb'].get('NewImage'):
        new_data = deserialize_dynamo(record['dynamodb']['NewImage'])
    else:
        logger.info("No new data to update, returning...")
        return default_response

    # print(new_data)
    if new_data.get("badge_type_display") != "State Park Site":
        logger.info("Only syncing state park sites, returning...")
        return default_response

    sp_ddb = StateParkSitesDB(config)
    site_id = new_data["site_id"]

    existing_site = sp_ddb.get_site(site_id)
    if existing_site:
        response = update_site(existing_site, new_data, sp_ddb)
    else:
        response = insert_site(new_data, sp_ddb)

    return response

@loghandler
def lambda_handler(event, context):
    config = {
        "DYNAMODB_REGION": "us-east-1",
        "env": "prod"
    }

    logger.info("Processing {} records:".format(len(event["Records"])))
    for record in event.get("Records"):
        response = handle_event(record, config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }
    return response