import os
import urllib.request

JAR_URL = 'https://storage.cloud.google.com/spark-mobility-jar/timegeo010.jar'
JAR_NAME = 'timegeo010.jar'
JAR_PATH = os.path.join(os.path.dirname(__file__), 'lib', JAR_NAME)

def ensure_jar():
    if not os.path.exists(JAR_PATH):
        os.makedirs(os.path.dirname(JAR_PATH), exist_ok=True)
        print(f"Downloading {JAR_NAME} from GCS...")
        urllib.request.urlretrieve(JAR_URL, JAR_PATH)
        print("Download complete.")
