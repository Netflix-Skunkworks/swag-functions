import os
import logging
from boto3.dynamodb.types import TypeDeserializer


from swag_client.backend import SWAGManager
from swag_client.schemas import v2
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
        'swag.schema_version': 1
    }

    swag = SWAGManager(**parse_swag_config_options(swag_opts))

    for record in event['Records']:
        logger.info('Processing stream record...')

        if record['eventName'] in ['INSERT', 'MODIFY']:
            new = record['dynamodb']['NewImage']
            data = {}
            for item in new:
                data[item] = deser.deserialize(new[item])

            data = v2.downgrade(data)
            if record['eventName'] == 'INSERT':
                swag.create(data)

            elif record['eventName'] == 'MODIFY':
                swag.update(data)

        if record['eventName'] == 'REMOVE':
            swag.delete({'id': 'aws-' + record['dynamodb']['Keys']['id']['S']})
