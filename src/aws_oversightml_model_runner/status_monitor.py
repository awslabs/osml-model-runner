import logging
from collections import namedtuple
from datetime import datetime, timezone

import boto3
from coral import coralrpc
from coral.simple_types import Timestamp

from com.amazon.oversightml.oversightml import OversightMLClient
from com.amazon.oversightml.processingevent import ProcessingEvent

Credentials = namedtuple('Creds', ['aws_access_key', 'aws_secret_key', 'aws_security_token'])


class StatusMonitor:

    def __init__(self, endpoint: str):

        logging.info("Configuring Status Monitor using Endpoint: {}".format(endpoint))
        if endpoint:
            session = boto3.Session()
            creds = session.get_credentials()
            region = session.region_name
            orchestrator = coralrpc.new_orchestrator(
                endpoint=endpoint,
                aws_region=region,
                aws_service="execute-api",
                aws_access_key=creds.access_key.encode('utf-8'),
                aws_secret_key=creds.secret_key.encode('utf-8'),
                aws_security_token=creds.token.encode('utf-8'),
                signature_algorithm="v4",
                timeout=1.0
            )
            self.cp_client = OversightMLClient(orchestrator=orchestrator)
        else:
            self.cp_client = None

    def processing_event(self, job_id: str, status: str, description: str):
        try:
            logging.info("StatusMonitor Update: {} {}: {}".format(status, job_id, description))
            if self.cp_client:
                self.cp_client.add_image_processing_event(
                    ProcessingEvent(event_description=description,
                                    event_time=Timestamp(datetime.now(tz=timezone.utc)),
                                    job_arn=job_id,
                                    job_status=status
                                    )
                )
        except Exception as status_error:
            logging.error("Unable to update OversightML CP for: {}".format(job_id), status_error)

