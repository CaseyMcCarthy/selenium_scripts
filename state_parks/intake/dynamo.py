import logging
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class DynamoError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

class TentrrDynamoDB():
    def __init__(self, config):
        self.config = config
        self.region = config["DYNAMODB_REGION"]
        self.conn = boto3.resource(
            "dynamodb", region_name=self.region)

class RAReservationDB(TentrrDynamoDB):
    def __init__(self, config):
        super().__init__(config)
        assert config["env"]
        self.table = self.conn.Table("RA_reservations_{}".format(config['env'].upper()))

    def _dynamo_error_wrapper(f):
        def wrapper(*args, **kwargs):
            try:
                response = f(*args, **kwargs)
            except ClientError as e:
                logger.error(e.response['Error']['Message'])
                raise DynamoError(e.response['Error']['Message'])
            else:
                logging.info(response)
                return response
        return wrapper

    @_dynamo_error_wrapper
    def get_reservation(self, reservation_id):
        logger.info("fetching reservation from RA dynamo...")
        response = self.table.get_item(Key={"id": reservation_id})
        return response.get("Item", None)

    @_dynamo_error_wrapper
    def set_reservation(self, reservation):
        logger.info("adding reservation to RA dynamo...")
        res = self.table.put_item(Item=reservation)
        return res

    @_dynamo_error_wrapper
    def update_reservation(self, reservation_id, data):
        logger.info("updating reservation in RA dynamo...")
        last_update = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        response = self.table.update_item(
            Key={
                'id': reservation_id
            },
            UpdateExpression="set #s=:s, start_day=:l, end_day=:e, fees=:f, occupancy=:o, listing_id=:a, site_name=:b",
            ExpressionAttributeValues={
                ':s': data["status"],
                ':l': data["start_day"],
                ':e': data["end_day"],
                ':f': data["fees"],
                ':o': data["occupancy"],
                ':a': data["listing_id"],
                ':b': data["site_name"]
            },
            ExpressionAttributeNames={'#s': 'status'},
            ReturnValues="UPDATED_NEW"
        )
        return response

class StateParkSitesDB(TentrrDynamoDB):
    def __init__(self, config):
        super().__init__(config)
        assert config["env"]
        self.table = self.conn.Table("state_park_sites_{}".format(config['env'].upper()))

    def _dynamo_error_wrapper(f):
        def wrapper(*args, **kwargs):
            try:
                response = f(*args, **kwargs)
            except ClientError as e:
                logger.error(e.response['Error']['Message'])
                raise DynamoError(e.response['Error']['Message'])
            else:
                logging.info(response)
                return response
        return wrapper

    @_dynamo_error_wrapper
    def get_site(self, name, park):
        logger.info("fetching site from State Park Sites dynamo...")
        response = self.table.query(
            FilterExpression=Key('park').eq(park),
            KeyConditionExpression=Key('ra_name').eq(name),
            IndexName="ra_name-index"
        )

        if response['Items'] and len(response['Items']) > 0:
            return response['Items'][0]
        else:
            return None