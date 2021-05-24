import boto3

dynamodb = boto3.resource('dynamodb')
feature_table = dynamodb.Table('ImageProcessingFeatures')
job_table = dynamodb.Table('ImageProcessingJobStatus')

scan = None
with feature_table.batch_writer() as batch:

    # Iterate through table until it's fully scanned
    while scan is None or 'LastEvaluatedKey' in scan:
        if scan is not None and 'LastEvaluatedKey' in scan:
            scan = feature_table.scan(
                ProjectionExpression='hash_key, range_key',
                ExclusiveStartKey=scan['LastEvaluatedKey'],
            )
        else:
            scan = feature_table.scan(ProjectionExpression='hash_key, range_key')

        for item in scan['Items']:
            print(item)
            batch.delete_item(Key={'hash_key': item['hash_key'], 'range_key': item['range_key']})


scan = None
with job_table.batch_writer() as batch:

    # Iterate through table until it's fully scanned
    while scan is None or 'LastEvaluatedKey' in scan:
        if scan is not None and 'LastEvaluatedKey' in scan:
            scan = job_table.scan(
                ProjectionExpression='image_id',
                ExclusiveStartKey=scan['LastEvaluatedKey'],
            )
        else:
            scan = job_table.scan(ProjectionExpression='image_id')

        for item in scan['Items']:
            print(item)
            batch.delete_item(Key={'image_id': item['image_id']})
