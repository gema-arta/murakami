import os
import io
import subprocess
import logging
import jsonlines

from google.cloud import storage
from murakami.exporter import MurakamiExporter

logger = logging.getLogger(__name__)


class GCSExporter(MurakamiExporter):
    """This exporter allows to upload data to a Google Cloud Storage
    bucket."""
    def __init__(
            self,
            name="",
            location=None,
            network_type=None,
            connection_type=None,
            config=None,
    ):
        super().__init__(
            name=name,
            location=location,
            network_type=network_type,
            connection_type=connection_type,
            config=config,
        )
        logging.debug(config)
        self.target = config.get("target", None)
        self.key = config.get("key", None)
        self.client = None

    def upload(self, data, bucket_name, object_name):
        if self.client is None:
            return

        bucket = self.client.bucket(bucket_name)
        blob = storage.Blob(object_name, bucket)
        blob.upload_from_string(data)


    def push(self, test_name="", data=None, timestamp=None):
        """Upload the test data to GCS using the provided configuration."""
        if self.target is None:
            logger.error("GCS: target must be provided.")
            return

        # Get a Google Cloud Storage Client object from the provided key.
        self.client = storage.Client.from_service_account_json(self.key)

        try:
            test_filename = self._generate_filename(test_name, timestamp)

            # Split the "target" configuration value into a bucket_name and
            # path. e.g. gs://bucket/path/to/results becomes:
            # - bucket_name: bucket
            # - path: path/to/results
            t = self.target.split('/')
            bucket_name = t[2]

            object_name = ''
            if len(t) > 3:
                object_name = '/'.join(t[3:])
                if t[3] != '':
                    object_name += '/'
            object_name += test_filename

            # Write JSONL output to a string.
            fp = io.StringIO()
            writer = jsonlines.Writer(fp)
            writer.write_all(data)
            writer.close()

            logger.info("Uploading test data - Bucket: %s, Object: %s",
                bucket_name, object_name)

            self.upload(fp.getvalue(), bucket_name, object_name)
        except ValueError as e:
            logger.error('Error while uploading to GCS: %s', e)