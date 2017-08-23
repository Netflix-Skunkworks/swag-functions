import os
import logging
from boto3.dynamodb.types import TypeDeserializer

from swag_client.backend import SWAGManager
from swag_client.migrations.versions import v2
from swag_client.util import parse_swag_config_options
from raven_python_lambda import RavenLambdaWrapper

logger = logging.getLogger('swag_client')
logger.setLevel(logging.DEBUG)

deser = TypeDeserializer()


@RavenLambdaWrapper()
def handler(event, context):
    """Forwards Dynamodb table events to S3 and downgrades them as needed."""
    swag_opts = {
        'swag.type': 's3',
        'swag.bucket_name': os.environ.get('SWAG_BUCKET_NAME'),
        'swag.region': os.environ.get('SWAG_BUCKET_REGION', 'us-east-1'),
        'swag.schema_version': 1
    }

    swag_v1 = SWAGManager(**parse_swag_config_options(swag_opts))

    # dual write both versions
    swag_opts_v2 = {
        'swag.type': 's3',
        'swag.bucket_name': os.environ.get('SWAG_BUCKET_NAME'),
        'swag.region': os.environ.get('SWAG_BUCKET_REGION', 'us-east-1'),
        'swag.data_file': 'v2/accounts.json',
        'swag.schema_version': 2
    }

    swag_v2 = SWAGManager(**parse_swag_config_options(swag_opts_v2))

    for record in event['Records']:
        logger.info('Processing stream record...')

        if record['eventName'] in ['INSERT', 'MODIFY']:
            new = record['dynamodb']['NewImage']
            data = {}
            for item in new:
                data[item] = deser.deserialize(new[item])

            data_v1 = v2.downgrade(data)
            if record['eventName'] == 'INSERT':
                swag_v2.create(data)
                swag_v1.create(data_v1)

            elif record['eventName'] == 'MODIFY':
                swag_v2.update(data)
                swag_v1.update(data_v1)

        if record['eventName'] == 'REMOVE':
            swag_v2.delete({'id': record['dynamodb']['Keys']['id']['S']})
            swag_v1.delete({'id': 'aws-' + record['dynamodb']['Keys']['id']['S']})
