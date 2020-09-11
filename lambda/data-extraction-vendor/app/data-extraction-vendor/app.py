from __future__ import annotations

import json
import logging
from enum import Enum, auto

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class NotificationMode(Enum):
    IMMEDIATE = auto()


def lambda_handler(event, _):
    try:
        for record in event['Records']:
            try:
                body = json.loads(record["body"])
                logger.info(f"SQS payload: {body}")
                tx_id_list = body['tx_id_list']
            except:
                logger.exception(f"ERROR - Record: {record}")
    except:
        logger.exception(f"ERROR - Event: {event}")
