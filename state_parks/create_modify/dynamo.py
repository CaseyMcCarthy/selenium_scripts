import logging
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    def update_reservation(self, reservation_id, guesty_id):
        logger.info("adding guesty ID in RA dynamo...")
        last_update = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        response = self.table.update_item(
            Key={
                'id': reservation_id
            },
            UpdateExpression="set guesty_id=:r, last_update=:l",
            ExpressionAttributeValues={
                ':r': guesty_id,
                ':l': last_update
            },
            ReturnValues="UPDATED_NEW"
        )
        return response