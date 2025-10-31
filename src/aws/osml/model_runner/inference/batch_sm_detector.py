# s3 event notification on s3://bucket/batch/output/


#     transform_response = smclient.create_transform_job(
#         TransformJobName="PyTorch-SimCamps-" + model_batch_time,
#         ModelName=model_name,
#         MaxPayloadInMB=100,
#         BatchStrategy="MultiRecord",
#         Environment={"batch_size": str(batch_size), "target_var": target_var},
#         TransformInput={
#             "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": batch_input}},
#             "ContentType": "text/csv",
#             "CompressionType": "None",
#             "SplitType": "Line",
#         },
#         TransformOutput={"S3OutputPath": batch_output, "KmsKeyId": os.environ["kmskeyid"]},
#         TransformResources={
#             "InstanceType": bt_instance_type,
#             "InstanceCount": 1,
#             "VolumeKmsKeyId": os.environ["kmskeyid"],
#         },
#     )


#         response = sagemaker_client.create_transform_job(
#         TransformJobName='my-batch-transform-job',
#         ModelName='my-trained-model',
#         TransformInput={
#             'DataSource': {
#                 'S3DataSource': {
#                     'S3DataType': 'S3Prefix',
#                     'S3Uri': 's3://your-input-bucket/input-data/'
#                 }
#             },
#             'ContentType': 'text/csv',
#             'SplitType': 'Line'
#         },
#         TransformOutput={
#             'S3OutputPath': 's3://your-output-bucket/output-data/',
#             'Accept': 'text/csv'
#         },
#         TransformResources={
#             'InstanceType': 'ml.m5.xlarge',
#             'InstanceCount': 1
#         },
#         # Optional: Add other parameters like MaxPayloadInMB, MaxConcurrentTransforms, Tags, etc.
#     )