import os

from schema import Optional, Schema


class ServiceConfig:
    def __init__(self):
        # set up expected ENV configuration schema
        self.schema = Schema(
            [
                {
                    "region": str,
                    "workers_per_cpu": int,
                    "job_table": str,
                    "feature_table": str,
                    "image_queue": str,
                    "region_queue": str,
                }
            ]
        )
        # these will throw and vail to validate if they aren't found and set correctly
        self.region = os.environ["AWS_DEFAULT_REGION"]
        self.workers_per_cpu = int(os.environ["WORKERS_PER_CPU"])
        self.job_table = os.environ["JOB_TABLE"]
        self.feature_table = os.environ["FEATURE_TABLE"]
        self.image_queue = os.environ["IMAGE_QUEUE"]
        self.region_queue = os.environ["REGION_QUEUE"]

        # optional params won't throw or fail to validate if they aren't found
        self.cp_api_endpoint = os.getenv("CP_API_ENDPOINT")

        # validate our configuration
        self.schema.validate(
            [
                {
                    "region": self.region,
                    "workers_per_cpu": self.workers_per_cpu,
                    "job_table": self.job_table,
                    "feature_table": self.feature_table,
                    "image_queue": self.image_queue,
                    "region_queue": self.region_queue,
                }
            ]
        )
