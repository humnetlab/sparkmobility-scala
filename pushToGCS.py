from google.cloud import storage

def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
    storage_client = storage.Client.from_service_account_json(
        '/data_1/albert/gcs_key.json')
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    
    return blob.public_url

upload_to_bucket("sparkmobility010.jar", "target/scala-2.13/timegeo010.jar", "sparkmobility")