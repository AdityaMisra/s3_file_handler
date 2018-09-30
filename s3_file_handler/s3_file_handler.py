import logging
import os

import xlrd
from boto.s3.connection import S3Connection

from s3_file_handler.constants import Constants


logger = logging.getLogger(__name__)


class S3FileTransfer(object):
    """
    This class can be used for
     - uploading files to S3 bucket
     - downloading files from S3 bucket
     - deleting file from S3 bucket
     - checking if a file exists in S3 bucket
    """

    def __init__(self, file_path, s3_folder_name):
        super(S3FileTransfer, self).__init__()
        self.s3_connection = S3Connection(Constants.AWS_ACCESS_KEY_ID, Constants.AWS_SECRET_ACCESS_KEY)

        self.bucket = self.s3_connection.get_bucket(Constants.AWS_STORAGE_BUCKET_NAME)

        # file_path can be the absolute path of the file or only the file_name as well
        # for uploading - pass the file path
        # for downloading - pass the file name
        # to check if a file exists in s3 - pass the file name
        self.file_path = file_path

        if s3_folder_name and Constants.TEST_ENV:
            self.s3_folder_name = s3_folder_name + '_test'
        else:
            self.s3_folder_name = s3_folder_name

    def upload_a_file_to_s3(self, sub_folder=''):
        """
         Uploads a file contents to S3 folder.
        :param sub_folder:
        :return:
        """

        try:
            logger.debug('Uploading to S3 bucket: {} and folder: {}'.format(Constants.AWS_STORAGE_BUCKET_NAME,
                                                                            self.s3_folder_name))

            # always get AWS_STORAGE_BUCKET_NAME bucket with validate False
            self.bucket = self.s3_connection.get_bucket(Constants.AWS_STORAGE_BUCKET_NAME, validate=False)

            # upload to ref folder.
            if os.path.exists(self.file_path):
                key_name = os.path.join(self.s3_folder_name, sub_folder, self.file_path.split('/')[-1])
                key = self.bucket.new_key(key_name)
                key.set_contents_from_filename(self.file_path)
                logger.info('Successfully uploaded file {} to S3:{}/{}'.format(self.file_path, self.s3_folder_name,
                                                                               key.name))
                return key_name
            else:
                logger.error('{} Does not exists'.format(self.file_path))

        except Exception as e:
            logger.exception('Exception in uploading file to s3. {}'.format(e))

        return None

    def download_a_file_from_s3(self):
        """
        Download a file from S3 folder
        :return:
        """
        logger.debug('Downloading from S3 bucket: {} and folder: {}'.format(Constants.AWS_STORAGE_BUCKET_NAME,
                                                                            self.s3_folder_name))

        # create a file for copying the content from s3
        path = os.path.join(Constants.S3_DOWNLOAD_DIR, os.path.dirname(self.file_path))
        if not os.path.exists(path):
            os.makedirs(path)

        s3_file_path = os.path.join(Constants.S3_DOWNLOAD_DIR, self.file_path)

        if not os.path.isfile(s3_file_path):
            key = self.bucket.get_key(self.s3_folder_name + "/" + self.file_path)
            if key:
                key.get_contents_to_filename(s3_file_path)
            else:
                return None

        return s3_file_path

    def download_file_from_s3_and_read_it(self):
        """
        Download a xls or xlsx file from S3 and read its content
        :return:
        """

        xls_file_path = self.download_a_file_from_s3()
        if not xls_file_path:
            logger.info("S3FileTransfer: xls_file_path is None")
            return None

        xls_file_obj = xlrd.open_workbook(xls_file_path, 'r')
        sh = xls_file_obj.sheet_by_index(0)

        # remove the file from s3_download folder
        if os.path.isfile(xls_file_path):
            os.remove(xls_file_path)

        return sh

    def delete_file_from_s3(self):
        """
        Delete a file from S3 folder
        :return:
        """
        try:
            key = self.bucket.get_key(os.path.join(self.s3_folder_name, self.file_path))
            self.bucket.delete_key(key)
            logger.info("Deleted file from S3 folder-{}, file_path-{}".format(self.s3_folder_name, self.file_path))
            return True
        except Exception as e:
            logger.exception("Error occurred while deleting file from s3: %s", e)
            return False

    def check_if_file_exists_in_s3_and_return_s3_file_path(self):
        """
        Method to check the existence of a file in a particular S3 folder
        :return: file_path in S3
        """
        file_exists = self._check_if_a_file_exists_in_s3()
        s3_file_path = None

        if file_exists:
            s3_file_path = os.path.join(self.s3_folder_name, self.file_path)

        return s3_file_path

    def _check_if_a_file_exists_in_s3(self):
        key = self.bucket.get_key(os.path.join(self.s3_folder_name, self.file_path))
        if key:
            return key.exists()
        return False
