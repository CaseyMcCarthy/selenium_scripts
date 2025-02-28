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
    def get_site(self, site_id):
        logger.info("fetching site from State Park Sites dynamo...")
        response = self.table.get_item(Key={"site_id": site_id})
        return response.get("Item", None)

    @_dynamo_error_wrapper
    def set_site(self, site):
        logger.info("adding site to State Park Sites dynamo...")
        response = self.table.put_item(Item=site)
        return response

class T4SiteDetails(TentrrDynamoDB):
    def __init__(self, config):
        super().__init__(config)
        assert config["env"]
        self.table = self.conn.Table("t4_site_details_{}".format(config['env'].upper()))

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
    def get_site_details(self, site_id):
        logger.info("fetching site details from t4 site details dynamo...")
        response = self.table.get_item(Key={"site_id": site_id})
        return response["Item"]

    @_dynamo_error_wrapper
    def get_site_details_by_listing(self, listing_id):
        logger.info("fetching site details by listing from t4 site details dynamo...")
        response = self.table.query(
            KeyConditionExpression=Key('guesty_listing_id').eq(listing_id),
            IndexName="guesty_listing_id-index"
        )
        if response['Items']:
            return response['Items'][0]
        else:
            # raise Exception(f"Site not found with listing {listing_id}")
            return None