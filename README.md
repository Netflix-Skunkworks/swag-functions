# ⚡️ Lambda Functions for SWAG

[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)

# About
These are a collection of serverless functions that are useful when used in conjunction with SWAG.

## s3-forwarder

This function is triggered off of a Dynamodb stream, processing insert and delete events,
then using the swag-client ensures that these changes are propagated the specified S3 bucket.

This is useful for cases where you want SWAG data to be located in Dynamodb but also offer up
a flat json file for consumption by the swag-client or other clients.

### IAM Permissions

In order to use the s3-forwarder Lambda ensure that you create a role with the following permissions:


#### S3

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1501177608000",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "<you-bucket-arn>"
            ]
        }
    ]
}
```


#### Dynamodb

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1501174124000",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams"
            ],
            "Resource": [
                "<your-stream-arn>"
            ]
        }
    ]
}
```


## Dynamodb

This serverless function will add Dynamodb stream trigger to our the s3-forwarder but will not create the stream for us,
ensure that `NewItem` is sent to the function through the Dynamodb stream.

## Monitoring

All of the functions are wrapped with the `RavenLambdaWrapper`. This decorator forwards lambda
telemetry to a [Sentry](https://sentry.io) instance. This will have no effect unless you specify `SENTRY_DSN`
in the Lambda's environment variables.