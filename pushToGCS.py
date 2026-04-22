"""Upload the assembled sparkmobility JAR to GCS.

The blob name must match what the Python sparkmobility package expects at
https://storage.googleapis.com/sparkmobility/sparkmobility-<version>.jar
(see sparkmobility/__init__.py:ensure_jar). Keep the version in lockstep
with build.sbt.
"""
import os

from google.cloud import storage

VERSION = "0.1.0"
JAR_FILENAME = f"sparkmobility-{VERSION}.jar"
LOCAL_JAR = f"target/scala-2.13/{JAR_FILENAME}"
BUCKET = "sparkmobility"

# Prefer GOOGLE_APPLICATION_CREDENTIALS; fall back to the well-known path
# used on the humnetlab dev box.
KEY_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", "/data_1/albert/gcs_key.json"
)


def upload_to_bucket(blob_name: str, path_to_file: str, bucket_name: str) -> str:
    client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url


if __name__ == "__main__":
    url = upload_to_bucket(JAR_FILENAME, LOCAL_JAR, BUCKET)
    print(f"Uploaded {LOCAL_JAR} → {url}")
