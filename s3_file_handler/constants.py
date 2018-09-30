import os


class AWSCredentials(object):
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


class Constants(AWSCredentials):
    AWS_STORAGE_BUCKET_NAME = 'MyBucket'
    S3_DOWNLOAD_DIR = 's3_folder'

    if os.environ.get('app_env') != 'prod':
        TEST_ENV = True
    else:
        TEST_ENV = False
