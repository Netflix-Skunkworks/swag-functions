"""
fowarder.py

Dumps the SWAG schema from DynamoDB to S3
"""
import os
import logging

import boto3
from boto3.dynamodb.types import TypeDeserializer
import simplejson as json

from retrying import retry, RetryError
from swag_client.backend import SWAGManager
from swag_client.migrations.versions.v2 import downgrade
from swag_client.util import parse_swag_config_options
from raven_python_lambda import RavenLambdaWrapper


logger = logging.getLogger('swag_functions')
logger.setLevel(logging.DEBUG)

deser = TypeDeserializer()


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def save_to_s3(bucket, region, data_file, body):
    client = boto3.client("s3", region_name=region)

    client.put_object(Bucket=bucket, Key=data_file, Body=body, ContentType='application/json',
                      CacheControl='no-cache, no-store, must-revalidate')


def dump_v2_to_s3(all_accounts, swag_opts):
    logger.debug("[ ] Dumping SWAG V2 Schema to S3...")
    save_to_s3(swag_opts["swag.bucket_name"], swag_opts["swag.region"], swag_opts["swag.data_file"],
               json.dumps(all_accounts))
    logger.info("[+] Completed dumping SWAG V2 to S3")


def dump_v2_to_v1_s3(namespace, all_accounts, swag_opts):
    v1_accounts = [downgrade(account) for account in all_accounts]
    data = {namespace: v1_accounts}

    logger.debug("[ ] Dumping SWAG V1 Schema to S3...")
    save_to_s3(swag_opts["swag.bucket_name"], swag_opts["swag.region"], swag_opts["swag.data_file"], json.dumps(data))
    logger.info("[+] Completed dumping SWAG V1 to S3")


@RavenLambdaWrapper()
def handler(event, context):
    logger.debug("[ ] Starting SWAG dump to S3...")
    """Forwards Dynamodb table events to S3 and downgrades them as needed."""
    swag_v1_s3 = {
        'swag.bucket_name': os.environ.get('SWAG_BUCKET_NAME'),
        'swag.region': os.environ.get('SWAG_BUCKET_REGION', 'us-east-1'),
        'swag.data_file': "accounts.json"
    }
    swag_opts_v2_s3 = {
        'swag.bucket_name': os.environ.get('SWAG_BUCKET_NAME'),
        'swag.region': os.environ.get('SWAG_BUCKET_REGION', 'us-east-1'),
        'swag.data_file': 'v2/accounts.json',
    }

    dynamo_opts = {
        'swag.type': "dynamodb",
        'swag.namespace': os.environ.get('SWAG_BACKEND_NAMESPACE', 'accounts')
    }
    swag_v2 = SWAGManager(**parse_swag_config_options(dynamo_opts))

    # Do a fresh dump of all the data:
    logger.debug("[-->] Fetching all accounts from Dynamo...")
    all_accounts = swag_v2.get_all()
    logger.debug("[+] Got all accounts from Dynamo.")

    try:
        dump_v2_to_s3(all_accounts, swag_opts_v2_s3)
        # And for V1:
        dump_v2_to_v1_s3(os.environ.get('SWAG_BACKEND_NAMESPACE', 'accounts'), all_accounts, swag_v1_s3)
    except RetryError as re:
        logger.error("[X] Error saving SWAG to S3. See exception for details.")
        raise re

    logger.debug("[+] Done!")
