"""
Misc classes used throughout SJ
"""
import os
import os.path
import shutil
import random
import string
import io

from django.conf import settings

import boto3
import botocore


# Configure S3 access now
session = boto3.session.Session()
s3 = session.resource('s3')

class SwappableS3File:
    """
    SwappableS3File can either store files in S3 or on the local file system.

    All files are in binary mode. The bucket given should either be as S3 bucket
    or will be treated as a subdirectory under the MEDIA_ROOT if in local file mode.

    The mode in use is determined by settings.USE_S3. If using AWS,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET should be set.
    If using local storage, MEDIA_ROOT will be used as the base.
    """
    @staticmethod 
    def get_available_name():
        """
        Returns an unused name in the MEDIA_ROOT or S3_BUCKET
        """
        while True:
            name = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(64))

            if settings.USE_S3:
                try:
                    s3.Object(settings.S3_BUCKET, name).load()
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        return name
                    else:
                        raise e
            
            else:
                # Check if the local file exists
                if not os.path.exists(os.path.join(settings.MEDIA_ROOT, name)):
                    return name

    @staticmethod
    def upload(src, fname=None):
        """
        Uploads the given file to either S3_BUCKET or MEDIA_ROOT.

        src must be an open file-like object. If fname isn't given, a random one will be used.

        Returns the filename used for the uploaded object.
        """
        # Get name as needed
        if fname is None:
            fname = SwappableS3File.get_available_name()

        if settings.USE_S3:
            s3.Bucket(settings.S3_BUCKET).upload_fileobj(src, fname)

        else:
            with open(os.path.join(settings.MEDIA_ROOT, fname), 'wb') as dest:
                shutil.copyfileobj(src, dest)
        
        return fname
    
    @staticmethod
    def download(fname):
        """
        Retrieves a file-like object from either S3_BUCKET or MEDIA_ROOT.
        """
        if settings.USE_S3:
            dest = io.BytesIO()
            s3.Bucket(settings.S3_BUCKET).download_fileobj(fname, dest)
            dest.seek(0)
            return dest
        else:
            return open(os.path.join(settings.MEDIA_ROOT, fname), 'rb')
