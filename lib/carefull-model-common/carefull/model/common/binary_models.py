import logging
import os
import pickle
from enum import Enum, auto

import boto3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

BUCKET_NAME = 'carefull-models'

s3_client = boto3.client('s3')

class BinaryModelType(Enum):
    VENDOR = auto()

    def file_name(self):
        return f"{self.name.lower()}_model.pkl"

    def local_path(self):
        return os.path.join(os.path.dirname(__file__), self.file_name())


class BinaryModelRepo:

    def save(self, model, type: BinaryModelType, local=False):
        if local:
            with open(type.local_path(), 'wb') as handle:
                pickle.dump(model, handle, )
        else:
            logger.info(f"Starting upload to S3 bucket:{BUCKET_NAME}, filename: {type.file_name()}")
            pickle_byte_obj = pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)
            s3_resource = boto3.resource('s3')
            s3_resource.Object(BUCKET_NAME, type.file_name()).put(Body=pickle_byte_obj)
            logger.info("Upload finish.")


    def load(self, type: BinaryModelType, local=False):
        if local:
            with open(type.local_path(), 'rb') as handle:
                return pickle.load(handle)
        else:
            logger.info(f"Starting loading from S3 bucket:{BUCKET_NAME}, filename: {type.file_name()}")
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=type.file_name())
            model = pickle.loads(response['Body'].read())
            logger.info("Download finish.")
            return model


if __name__ == '__main__':
    repo = BinaryModelRepo()
    vendor_model = repo.load(BinaryModelType.VENDOR)


