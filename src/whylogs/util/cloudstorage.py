"""
Define utility functions for interacting with remote cloud object storage
(i.e. AWS S3)
"""
import threading
from logging import getLogger

import boto3

# XXX : not sure if we should use RLock or Lock here
_s3client_rlock = threading.RLock()


def check_bucket(bucket_name):
    """
    Check access to a cloud storage bucket.

    Parameters
    ----------
    bucket_name : str
        The S3 bucket

    Returns
    -------
    success : bool
        Whether the bucket can be accessed
    """
    s3 = boto3.resource("s3")
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        success = True
    except Exception as e:
        getLogger(__name__).info(f"Could not access bucket: {e}")
        success = False
    return success


def s3client(**kwargs):
    """
    Returns a client in a threadsafe manner.

    Returned clients are threadsafe

    Parameters
    ----------
    kwargs : optional
        Passed to the `boto3.client` constructor

    Returns
    -------
    client :
        The boto3 S3 client
    """
    with _s3client_rlock:
        client = boto3.client("s3", **kwargs)
    return client


def list_keys(bucket, prefix="", suffix="", client=None):
    """
    Generator to list keys in an S3 bucket.

    Parameters
    ----------
    bucket : str
        Name of the cloud bucket.
    prefix : str
        Only fetch keys that start with this prefix.
    suffix : str
        Only fetch keys that end with this suffix.
    client :
        If specified, the boto3 s3 client
    """
    for obj in list_objects(bucket, prefix, suffix, client=client):
        yield obj["Key"]


def list_objects(
    bucket: str,
    prefix: str = None,
    suffix: str = None,
    start_after: str = None,
    client=None,
):
    """
    Generator to list objects in an S3 bucket.

    Parameters
    ----------
    bucket : str
        Name of the cloud bucket.
    prefix : str
        Only fetch keys that start with this prefix.
    suffix : str
        Only fetch keys that end with this suffix.
    start_after : str
        Where you want Amazon S3 to start listing from. Amazon S3 starts
        listing after this specified key. StartAfter can be any key in the
        bucket
    client :
        If specified, the boto3 s3 client
    """
    if prefix is None:
        prefix = ""
    if suffix is None:
        suffix = ""
    if client is None:
        client = s3client()
    kwargs = {"Bucket": bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs["Prefix"] = prefix
    if start_after:
        kwargs["StartAfter"] = start_after

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = client.list_objects_v2(**kwargs)

        try:
            contents = resp["Contents"]
        except KeyError:
            return

        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
