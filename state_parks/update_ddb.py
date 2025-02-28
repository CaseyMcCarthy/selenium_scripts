import json
import boto3
import logging
from boto3 import resource
from decimal import Decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()

CART = {
    "session_id": "044326ea-630f-4654-8f41-427140168e48",
    "listing_id": "5e7e744d18a634002d4d835c",
    "site_id": "4b9d7dc1-a3ad-11e8-ab37-0adc64bdfa55",
    "start_date": "2021-05-28",
    "end_date": "2021-05-31",
    "occupancy_count": 4,
    "nights": [
        {
            "date": "2021-05-28",
            "price": 40
        },
        {
            "date": "2021-05-29",
            "price": 40
        },
        {
            "date": "2021-05-30",
            "price": 40
        }
    ],
    "night_count": 3,
    "extras": [
        {
            "article_type": "BILLABLE",
            "is_active": True,
            "charge_frequency": "ONE_TIME",
            "article_name": "Kayak Rental",
            "description": "Kayak our pond! One adult and one child kayak is available with paddles. There is a small rustic dock, hang out and enjoy the sunshine and the rustling reeds.",
            "relation_type": "site",
            "decouple": False,
            "article_id": "fc98375c-2e62-11e9-a38e-0adc64bdfa55",
            "assets": [
                {
                    "asset_category": "ARTICLE",
                    "updated_at": "02-15-2019 19:26:1550258789",
                    "asset_type": "BILLABLE_ARTICLE",
                    "created_at": "02-15-2019 19:26:1550258789",
                    "_id": "5c6806087a2d2364daef8aab",
                    "asset_id": "cfd7b5d4-31e8-11e9-a391-0adc64bdfa55",
                    "asset_path": "https://s3.amazonaws.com/ugc.tentrr/fc98375c-2e62-11e9-a38e-0adc64bdfa55/cfd7b5d6-31e8-11e9-a391-0adc64bdfa55_BILLABLE_ARTICLE.jpeg"
                }
            ],
            "billable_rate": 40,
            "is_public": True,
            "_id": "5c621d057a2d234f41115423",
            "category": 0,
            "sort_order": 4,
            "date": "2021-05-28"
        },
        {
            "article_type": "BILLABLE",
            "is_active": True,
            "charge_frequency": "ONE_TIME",
            "article_name": "Farm Tour",
            "description": "Learn how to be a sustainable farmer on our small farm. Caring and learning about chickens, ducks, and Icelandic sheep (or pigs when available)! Get hands on and learn about your food.",
            "relation_type": "site",
            "decouple": False,
            "article_id": "42fe9029-31e9-11e9-954a-0a932d599b6f",
            "assets": [
                {
                    "asset_category": "ARTICLE",
                    "updated_at": "02-07-2019 15:14:1549552462",
                    "asset_type": "BILLABLE_ARTICLE",
                    "created_at": "02-07-2019 15:14:1549552462",
                    "_id": "5c6807017a2d234f411154b5",
                    "asset_id": "63b9c594-31e9-11e9-a391-0adc64bdfa55",
                    "asset_path": "https://s3.amazonaws.com/ugc.tentrr/42fe9029-31e9-11e9-954a-0a932d599b6f/63b9c597-31e9-11e9-a391-0adc64bdfa55_BILLABLE_ARTICLE.jpeg"
                }
            ],
            "billable_rate": 35,
            "is_public": True,
            "_id": "5c6806c9972c3b13f281316e",
            "category": 0,
            "sort_order": 5,
            "date": "2021-05-28"
        }
    ],
    "extras_total": 7500,
    "discounts": [],
    "promo_code_total": 0,
    "gift_card_total": 0,
    "discount_total": 0,
    "tentrr_fee": 2925.0,
    "extra_camper_fee": 0,
    "accommodation_total": 12000,
    "total_charges": 22425.0,
    "status": "confirmed",
    "confirmation_number": "6W6o7nD17",
    "guest": {
        "first_name": "Joel",
        "last_name": "Kress",
        "phone": "13474395529",
        "email": "jskress23@gmail.com",
        "guesty_id": "60058c1e2a7ebf002c145a92"
    },
    "financials": {
        "payment_method_id": "60058c23faccd1002e8e4df0",
        "payment_method": "pm_1IAxirENKoCLO9PZ6kJ8DhIY"
    },
    "additional_fees": [],
    "additional_fee_total": 0,
    "cancel_policy": None,
    "cancel_policies": []
}


def update_details(res_id, details, table):
    logger.info("updating reservation details in dynamo...")
    last_updated_by = "cart"
    scrubbed = json.loads(json.dumps(details), parse_float=Decimal)
    try:
        response = table.update_item(
            Key={
                'id': res_id
            },
            UpdateExpression="set extras=:e, discounts=:d, extra_camper_fee=:c, financials=:f, additional_fees=:a, last_updated_by=:l, cart=:b",
            ExpressionAttributeValues={
                ':e': scrubbed["extras"],
                ':d': scrubbed["discounts"],
                ':c': scrubbed["extra_camper_fee"],
                ':f': scrubbed["financials"],
                ':a': scrubbed["additional_fees"],
                ':l': last_updated_by,
                ':b': scrubbed["cart"]
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info("details updated...")
    except ClientError as e:
        logging.error(e.response["Error"]["Message"])

def save_to_dynamo(res_id, table, cart):
    print("saving to dynamo...")
    try:
        details = {}
        details["extras"] = cart["extras"]
        details["discounts"] = cart["discounts"]
        details["extra_camper_fee"] = cart["extra_camper_fee"]
        details["financials"] = cart["financials"]
        details["additional_fees"] = cart["additional_fees"]
        details["cart"] = cart

        update_details(res_id, details, table) 
        logger.info("Saved to DDB")
    except Exception as e:
        logger.info("Failed to save reservation to DDB: {}".format(str(e)))
        pass
    return

def main():
    client = resource("dynamodb", region_name='us-east-1')
    table = client.Table("reservation_details_PROD")

    cart = CART
    res_id = "60058c1f2a7ebf002c145a95"
    
    save_to_dynamo(res_id, table, cart)
    
    
    return

if __name__ == "__main__":
    main()

