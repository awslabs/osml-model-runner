import logging
from datetime import datetime, timezone
from typing import Optional

import botocore.session

from aws_oversightml_model_runner.utils.constants import BOTO_CONFIG


class StatusMonitor:
    def __init__(self, endpoint: Optional[str]):
        logging.info("Configuring Status Monitor using Endpoint: {}".format(endpoint))
        if endpoint:
            session = botocore.session.get_session()
            self.cp_client = session.create_client(
                "oversightml", endpoint_url=endpoint, config=BOTO_CONFIG
            )
        else:
            self.cp_client = None

    def processing_event(self, job_id: str, status: str, description: str):
        try:
            logging.info("StatusMonitor Update: {} {}: {}".format(status, job_id, description))
            if self.cp_client:
                self.cp_client.add_image_processing_event(
                    eventDescription=description,
                    eventTime=datetime.now(tz=timezone.utc),
                    jobArn=job_id,
                    jobStatus=status,
                )
        except Exception:
            logging.exception("Unable to update OversightML CP for: {}".format(job_id))
