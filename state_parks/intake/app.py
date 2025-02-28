import json
import logging
import redis
from decimal import Decimal
from datetime import datetime

from tentrr.log import loghandler
from dynamo import RAReservationDB, StateParkSitesDB

from datadog_lambda.wrapper import datadog_lambda_wrapper
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
patch_all()

xray_recorder.configure(context_missing='LOG_ERROR')
logging.basicConfig(level='INFO')
logging.getLogger('aws_xray_sdk').setLevel(logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

REDIS_HOST = 'stateparks.kwuyrj.ng.0001.use1.cache.amazonaws.com'

    
def set_cache(res_id, status):
    logger.info(f"Setting cache for {res_id}")
    pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0)
    r = redis.Redis(connection_pool=pool)
    response = r.set(res_id, status, ex=43200)
    
    print(f"REDIS RESPONSE: {response}")
    
    return

def map_park(site_name, park_name, config):
    sp_ddb = StateParkSitesDB(config)
    site = sp_ddb.get_site(site_name, park_name)

    if not site:
        logger.error("Unable to map site")
        raise Exception(f"State Park site not found {park_name}: {site_name}")

    site_id = site["site_id"]
    listing_id = site["listing_id"]
    
    return site_id, listing_id

def format_data(res, config):
    site_name = (res["site_name"].split("-"))[0]
    park_name = res["park_name"]
    if park_name == "Lake DArbonne State Park":
        park_name = "Lake D'Arbonne State Park"
    site_id, listing_id = map_park(site_name, park_name, config)

    customer = res["customer"].split(",")
    first_name = customer[1]
    last_name = customer[0]
    full_name = f"{first_name} {last_name}"

    start_day = datetime.strftime(datetime.strptime(res["arrival"], "%b %d, %Y"), "%Y-%m-%d")
    end_day = datetime.strftime(datetime.strptime(res["departure"], "%b %d, %Y"), "%Y-%m-%d")
    
    fees = res["use_fees"].strip("$")
    fees = fees.replace(",", "")
    
    data = {
        "id": res["res_number"],
        "invoice_number": res["invoice_number"],
        "status": res["res_status"],
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "phone": str(res["camper_phone"]),
        "email": res["camper_email"],
        "type": res["type"],
        "occupancy": res["occupancy"],
        "site_name": site_name,
        "park": park_name,
        "state": res["state"],
        "fees": Decimal(fees),
        "start_day": start_day,
        "end_day": end_day,
        "site_id": site_id,
        "listing_id": listing_id,
        "source": "state_parks"
    }
    # print(data)
    return data

def handle_event(record, config):
    logger.info(record["Sns"]["Message"])
    raw_data = json.loads(record["Sns"]["Message"])
    ra_ddb = RAReservationDB(config)

    data = format_data(raw_data, config)
    
    # print(data)

    state_park_reservation_id = data["id"]
    
    set_cache(state_park_reservation_id, data["status"])
    
    prev_entry = ra_ddb.get_reservation(state_park_reservation_id)

    if prev_entry:
        if (
            prev_entry["status"] == data["status"]
            and prev_entry["start_day"] == data["start_day"]
            and prev_entry["end_day"] == data["end_day"]
            and prev_entry["fees"] == data["fees"]
            and prev_entry["occupancy"] == data["occupancy"]
            and prev_entry["listing_id"] == data["listing_id"]
            and prev_entry["site_name"] == data["site_name"]
        ):
            logger.info("No change to previous entry, returning...")
        else:
            logger.info("Reservation data has changed, updating ddb...")
            ra_ddb.update_reservation(state_park_reservation_id, data)     
    else:
        logger.info("No previous entry into RA DDB, adding...")
        ra_ddb.set_reservation(data)

    return default_response

@datadog_lambda_wrapper
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