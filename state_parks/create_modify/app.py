import json
import logging
import boto3
from time import sleep
from datetime import datetime

from tentrr import tentrr_lambda
from guesty.utils import (
    create_guesty_reservation,
    get_guesty_reservation,
    send_guesty_cash_payment,
    query_guesty_reservations,
    query_conflicting_reservations,
    update_guesty_reservation_status,
    cancel_guesty_reservation,
    refund_guesty_payment,
    update_guesty_reservation
)

from dynamo import RAReservationDB

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
deserializer = boto3.dynamodb.types.TypeDeserializer()

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

unsupported = {
    "statusCode": 400,
    "body": json.dumps("Unsupported event")
}

failure = {
    "statusCode": 500,
    "body": json.dumps("Unexpected error")
}

def deserialize_dynamo(record):
    return {k: deserializer.deserialize(v) for k, v in record.items()}

def save_to_guesty(data, ignore_calendar=True):
    """ Constructs and sends reservation object to Guesty via API
        if previous reservation not found

        Parameters:
        -----------
        data: dict
            Incoming state park reservation data
        ignore_calendar: bool, optional
            Updates ignoreCalendar flag to Guesty (default is False)
    """

    # Query Guesty for existing reservation
    existing = query_guesty_reservations(data["start_day"], data["end_day"], data["listing_id"], data["email"])
    if existing:
        logger.info("Existing reservation found")
        reservation = get_guesty_reservation(existing["_id"])
    else:
        payload = {}
        payload["listingId"] = data["listing_id"]
        payload["checkInDateLocalized"] = data["start_day"]
        payload["checkOutDateLocalized"] = data["end_day"]
        payload["guestsCount"] = int(data["occupancy"])
        payload["source"] = data["source"]
        payload["status"] = "reserved"
        payload["ignoreTerms"] = True
        payload["ignoreCalendar"] = ignore_calendar
        payload["guest"] = {}
        payload["guest"]["fullName"] = data["full_name"].strip()
        payload["guest"]["firstName"] = data["first_name"].strip()
        payload["guest"]["lastName"] = data["last_name"].strip()
        payload["guest"]["emails"] = []
        payload["guest"]["emails"].append(data["email"])
        payload["guest"]["phones"] = []
        payload["guest"]["phones"].append(data["phone"])
        payload["money"] = {}
        payload["money"]["currency"] = "USD"
        payload["money"]["fareAccommodation"] = float(data["fees"])
        payload["customFields"] = []
        payload["customFields"].append(
            {"value": data["id"], "fieldId": "6026d61227074e0030cc5840"}
        )

        reservation = create_guesty_reservation(payload)

    return reservation

def update_to_guesty(data, ignore_calendar=False):
    """ Constructs and sends updated reservation object to Guesty via API

        Parameters:
        -----------
        data: dict
            Incoming state park reservation data
        ignore_calendar: bool, optional
            Updates ignoreCalendar flag to Guesty (default is False)
    """
    payload = {}
    payload["listingId"] = data["listing_id"]
    payload["checkInDateLocalized"] = data["start_day"]
    payload["checkOutDateLocalized"] = data["end_day"]
    payload["guestsCount"] = int(data["occupancy"])
    payload["source"] = data["source"]
    payload["ignoreCalendar"] = ignore_calendar
    payload["guest"] = {}
    payload["guest"]["fullName"] = data["full_name"].strip()
    payload["guest"]["firstName"] = data["first_name"].strip()
    payload["guest"]["lastName"] = data["last_name"].strip()
    payload["guest"]["emails"] = []
    payload["guest"]["emails"].append(data["email"])
    payload["guest"]["phones"] = []
    payload["guest"]["phones"].append(data["phone"])
    payload["money"] = {}
    payload["money"]["currency"] = "USD"
    payload["money"]["fareAccommodation"] = float(data["fees"])

    reservation = update_guesty_reservation(data["guesty_id"], payload)

    return reservation

def get_latest_payment_id_amount(guesty_res):
    payments = guesty_res["money"]["payments"]
    holds = [k for k in payments if k["status"] == "SUCCEEDED"]
    latest_payment = sorted(holds, key=lambda pmt: pmt["createdAt"])[-1]

    return latest_payment["_id"], latest_payment["amount"]

def cancel_reservation(new_data):
    guesty_reservation_id = new_data["guesty_id"]
    try:
        cancel_guesty_reservation(guesty_reservation_id, "camper")

        reservation = get_guesty_reservation(new_data["guesty_id"])
        payment_id, amount = get_latest_payment_id_amount(reservation)

        refund_guesty_payment(guesty_reservation_id, payment_id, amount)
    except Exception as e:
        logger.error(f"failed to cancel reservation: {guesty_reservation_id}")
    finally:
        return

def modify_reservation(new_data):
    guesty_reservation_id = new_data["guesty_id"]
    try:
        reservation = update_to_guesty(new_data)

        balance = reservation["money"]["balanceDue"]
        if balance > 0:
            send_guesty_cash_payment(guesty_reservation_id, balance)
        elif balance < 0:
            payment_id = get_latest_payment_id_amount(reservation)[0]
            refund_guesty_payment(reservation_id, payment_id, abs(balance))
    except Exception as e:
        logger.error(f"failed to modify reservation: {guesty_reservation_id}")
    finally:
        return

def decline_conflicting_reservations(reservation):
    # conflicts = query_conflicting_reservations(reservation)
    # print("DDB RESPONSE: ")
    # print(json.dumps(conflicts))
    # if not conflicts:
    pass

def handle_event(record, config):
    logger.info(record["eventName"])
    event_type = record["eventName"]
    ra_ddb = RAReservationDB(config)

    if record['dynamodb'].get('NewImage'):
        new_data = deserialize_dynamo(record['dynamodb']['NewImage'])

    if record['dynamodb'].get('OldImage'):
        old_data = deserialize_dynamo(record['dynamodb']['OldImage'])

    if event_type == "INSERT":
        if new_data.get("guesty_id"):
            logger.info("Insert events shouldn't have guesty_ids, returning...")
            return default_response

        if new_data["status"] != "Pre Arrival":
            logger.info("Not creating reservations with status {}".format(new_data['status']))
            return default_response

        reservation = save_to_guesty(new_data)

        balance = reservation["money"]["balanceDue"]
        guesty_reservation_id = reservation["_id"]
        if balance > 0:
            send_guesty_cash_payment(guesty_reservation_id, balance)

        if reservation["status"] != "confirmed":
            sleep(5)
            update_guesty_reservation_status(guesty_reservation_id, "confirmed")

        state_park_reservation_id = new_data["id"]
        res = ra_ddb.update_reservation(state_park_reservation_id, guesty_reservation_id)

        # query and decline conflicting reservations
        # decline_onflicting_reservations(reservation)

    elif event_type == "MODIFY":

        if not old_data.get("guesty_id"):
            logger.info("Update from reservation creation, returning...")
            return default_response

        if old_data["status"] != new_data["status"]:
            logger.info("Status change")
            if new_data["status"] in ["Cancelled", "Void"]:
                logger.info("RA reservation cancelled")
                cancel_reservation(new_data)
            else:
                logger.info("Status changed to {}: no update needed...".format(new_data["status"]))

        if (
            old_data["start_day"] != new_data["start_day"]
            or old_data["end_day"] != new_data["end_day"]
            or old_data["occupancy"] != new_data["occupancy"]
            or old_data["listing_id"] != new_data["listing_id"]
            or old_data["site_name"] != new_data["site_name"]
        ):
            logger.info("Reservation has been updated")
            modify_reservation(new_data)
        else:
            logger.info("No changes to process, returning...")
    elif event_type == "REMOVED":
        pass
    else:
        logger.info(f"Unsupported event type: {event_type}")
        return unsupported

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