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
    def pull_all_reservation_ids(self):
        logger.info("pulling all reservations from RA dynamo...")
        response = self.table.scan()
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        reservations = [res.get("id") for res in data]

        return reservations

    @_dynamo_error_wrapper
    def remove_reservation(self, reservation_id):
        logger.info(f"removing reservation {reservation_id} from RA dynamo...")
        response = self.table.delete_item(Key={'id': reservation_id})
        logger.info(response)

        return