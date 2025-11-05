# -*- coding: utf-8 -*-
"""
S3 module for handling bucket and object operations.

This module provides a wrapper class around the boto3 S3 client.
"""

import logging
import threading
from pathlib import Path

import boto3
import botocore
from rich import progress

from .utils import time_elapsed


# pylint: disable=too-few-public-methods
class Config:
    """
    Handles configuration for AWS services by initializing a boto3 session.

    This class supports initializing configurations using either an AWS profile
    or environment variables. It uses boto3’s default credential chain.

    Attributes:
        session (boto3.Session): A boto3 Session object initialized.
        region_name (str): The AWS region name.
        s3_endpoint (str): The custom S3 endpoint URL.
    """

    def __init__(self, profile_name=None, region_name=None, s3_endpoint=None):
        """
        Initialize configurations using AWS profile or environment variables.

        Arguments:
            profile_name (str, optional): The name of the AWS profile to use.
            region_name (str, optional): The AWS region name to use.
            s3_endpoint (str, optional): The custom S3 endpoint URL.
        """
        self.region_name = region_name
        self.s3_endpoint = s3_endpoint

        try:
            self.session = boto3.Session(
                profile_name=profile_name, region_name=region_name
            )

            # Ensure credentials exist
            if not self.session.get_credentials():
                raise ValueError(
                    "Could not find AWS credentials. "
                    "Check environment variables or AWS profile."
                )
        except botocore.exceptions.ProfileNotFound as exc:
            raise ValueError(
                f"AWS profile '{profile_name}' not found. "
                "Check your ~/.aws/config and ~/.aws/credentials files."
            ) from exc
        except Exception as exc:
            raise ValueError(f"Error initializing AWS session: {exc}") from exc

    def get_session(self):
        """
        Returns the boto3 session.

        Returns:
            boto3.Session: The initialized boto3 session.
        """
        return self.session


# pylint: disable=too-few-public-methods
class RichProgressCallback:
    """A callback handler to update a Rich progress bar."""

    def __init__(self, progress_obj, task_id):
        """
        Initializes the callback handler.

        Arguments:
            progress_obj (rich.progress.Progress): The Rich Progress instance to update.
            task_id (rich.progress.TaskID): The task ID of the Progress
                                            instance that this callback will modify.
        """
        self._progress = progress_obj
        self._task_id = task_id
        self._lock = threading.Lock()

    def __call__(self, bytes_transferred):
        """
        The callback method invoked by boto3.

        Arguments:
            bytes_transferred (int): The number of bytes transferred
                                     since the last call.
        """
        with self._lock:
            self._progress.update(self._task_id, advance=bytes_transferred)


class S3Manager:
    """
    Class to handle S3 operations.

    Attributes:
        s3_resource (boto3.resource): The boto3 S3 resource object used
            to interact with S3.
        disable_pbar (bool): Flag to disable the progress bar display.
    """

    def __init__(self, config, checksum_policy=None):
        """
        Initializes the S3 manager with configurations provided by the Config object.

        Arguments:
            config (Config): The configuration object providing the session,
                             region, and S3 endpoint information.
        """
        boto3_session = config.get_session()

        boto3_config = botocore.config.Config(
            request_checksum_calculation=checksum_policy,
            response_checksum_validation=checksum_policy,
            read_timeout=300,
        )

        self.s3_resource = boto3_session.resource(
            "s3",
            endpoint_url=config.s3_endpoint,
            region_name=config.region_name,
            config=boto3_config,
        )
        self.disable_pbar = False

    def check_bucket_exist(self, bucket_name):
        """
        Checks if the specified bucket exists and caches the result.

        Arguments:
            bucket_name           (str): Bucket name

        Returns:
            bool: True if the bucket exists, False otherwise.
        """
        try:
            logging.debug("Checking if bucket exists: %s", bucket_name)
            self.s3_resource.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            raise

    def check_bucket_versioning(self, bucket_name):
        """
        Returns bucket versioning status.

        Arguments:
            bucket_name           (str): Bucket name

        Returns:
            str: Versioning status ("Enabled", "Suspended", or None).
        """
        return self.s3_resource.BucketVersioning(bucket_name).status

    def create_bucket(self, bucket_name, is_versioned=False):
        """
        Create a new S3 bucket.

        Arguments:
            bucket_name (str): The name of the bucket to create.
            is_versioned (bool): Enable versioning if True.
        """
        region = self.s3_resource.meta.client.meta.region_name
        if region and region != "us-east-1":
            bucket = self.s3_resource.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        else:
            bucket = self.s3_resource.create_bucket(Bucket=bucket_name)

        bucket.wait_until_exists()

        if is_versioned:
            logging.info("Enabling versioning for '%s'", bucket_name)
            output = bucket.Versioning().enable()
            logging.debug(output)
            logging.info("Versioning enabled")

        return bucket

    def list_buckets(self):
        """
        List all buckets.

        Returns:
            A list of Bucket resources
        """
        return self.s3_resource.buckets.all()

    def delete_bucket(self, bucket_name):
        """
        Delete an S3 bucket.
        The bucket must be empty.

        Arguments:
            bucket_name (str): The name of the bucket to delete.
        """
        # Get the bucket resource object
        bucket = self.s3_resource.Bucket(bucket_name)

        result = bucket.delete()
        logging.debug("Bucket delete response: %s", result)

        bucket.wait_until_not_exists()

    def _list_objects(self, collection_type, bucket_name, *, prefix=None, limit=None):
        """
        Helper to list items from a specified S3 bucket collection.

        This method consolidates the common logic for listing both current objects
        and object versions in a bucket. It dynamically accesses the corresponding
        Boto3 collection (e.g., ``objects`` or ``object_versions``) on the bucket
        resource and applies optional filtering and limiting.

        Arguments:
            collection_type   (str): The name of the Boto3 collection attribute to
                                     access, such as 'objects' or 'object_versions'
            bucket_name       (str): The name of the target S3 bucket
            prefix            (str): A string used to filter items that begin
                                     with this prefix. Defaults to None
            limit             (int): The maximum number of items to return.
                                     Defaults to None

        Returns:
            boto3.resources.collection.s3.BucketCollection: An iterable
                collection of S3 resources (like ObjectSummary or ObjectVersion)
                that match the specified criteria.
        """
        bucket = self.s3_resource.Bucket(bucket_name)
        collection = getattr(bucket, collection_type)

        if prefix:
            return collection.filter(Prefix=prefix).limit(limit)

        return collection.all().limit(limit)

    def list_objects(self, bucket_name, *, prefix=None, limit=None):
        """
        List objects stored in a bucket.

        Arguments:
            bucket_name      (str): Bucket name

        Keyword arguments (opt):
            prefix           (str): Filter only objects with specific prefix
                                    default None
            limit            (int): Limit the number of objects returned
                                    default None

        Returns:
            An iterable of ObjectSummary resources
        """
        return self._list_objects("objects", bucket_name, prefix=prefix, limit=limit)

    def list_objects_versions(self, bucket_name, *, prefix=None, limit=None):
        """
        List all objects versions stored in a bucket.

        Arguments:
            bucket_name      (str): Bucket name

        Keyword arguments (opt):
            prefix           (str): Filter only objects with specific prefix
                                    default None
            limit            (int): Limit the number of objects returned
                                    default None

        Returns:
            An iterable of ObjectVersion resources
        """
        return self._list_objects(
            "object_versions", bucket_name, prefix=prefix, limit=limit
        )

    def metadata_object(self, bucket_name, object_name, version_id=None):
        """
        Return object metadata.

        Arguments:
            bucket_name  (str): Bucket name
            object_name  (str): Object key name
            version_id   (str): Object version ID
        """
        params = {"Bucket": bucket_name, "Key": object_name}
        if version_id:
            params["VersionId"] = version_id

        return self.s3_resource.meta.client.head_object(**params)

    def delete_object(self, bucket_name, object_name, version_id=None):
        """
        Delete an object.

        Arguments:
            bucket_name  (str): Bucket name
            object_name  (str): Object key name
            version_id   (str): Object version ID
        """
        client = self.s3_resource.meta.client

        if version_id:
            return client.delete_object(
                Bucket=bucket_name,
                Key=object_name,
                VersionId=version_id,
            )

        return client.delete_object(
            Bucket=bucket_name,
            Key=object_name,
        )

    @time_elapsed
    def upload_file(self, bucket_name, file_name, key_name=None):
        """
        Upload a file from local source to S3.

        Arguments:
            bucket_name        (str): The name of the bucket to upload to
            file_name          (str): The path to the file to upload
            key_name           (str): The name of the key to upload to
                                      If key_name is None, the file_name
                                      is used as object name
        """
        if key_name is None:
            key_name = file_name

        logging.debug("Uploading file: %s with key: %s", file_name, key_name)

        obj_size = Path(file_name).stat().st_size
        with progress.Progress(
            progress.TextColumn("[progress.description]{task.description}"),
            progress.BarColumn(),
            progress.TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            progress.TransferSpeedColumn(),
            progress.TimeRemainingColumn(),
            progress.TimeElapsedColumn(),
            disable=self.disable_pbar,
        ) as pbar:
            task_id = pbar.add_task(f"Uploading [green]{file_name}[/]", total=obj_size)
            callback = RichProgressCallback(pbar, task_id)
            self.s3_resource.Bucket(bucket_name).upload_file(
                Filename=file_name,
                Key=key_name,
                Callback=callback,
            )

    @time_elapsed
    def download_object(self, bucket_name, object_name, dest_name, versionid=None):
        """
        Download an object from S3 to local source.

        Arguments:
            bucket_name  (str): Bucket name
            object_name  (str): Object name
            dest_name    (str): Full path filename to store the object
            versionid    (str): Object version ID
        """
        logging.debug("Downloading object %s to dest %s", object_name, dest_name)

        if versionid:
            extraargs = {"VersionId": versionid}
            resp = self.s3_resource.ObjectVersion(
                bucket_name, object_name, versionid
            ).head()
            obj_size = resp["ContentLength"]
            # Change to 'size' attribute when this boto3 bug got fixed:
            # https://github.com/boto/boto3/issues/832
            # self.s3_resource.ObjectVersion(bucket_name, object_name, versionid).size
        else:
            extraargs = None
            obj_size = self.s3_resource.ObjectSummary(bucket_name, object_name).size

        logging.debug("obj_size: %s, extraargs: %s", obj_size, extraargs)

        with progress.Progress(
            progress.TextColumn("[progress.description]{task.description}"),
            progress.BarColumn(),
            progress.TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            progress.TransferSpeedColumn(),
            progress.TimeRemainingColumn(),
            disable=self.disable_pbar,
        ) as pbar:
            task_id = pbar.add_task(
                f"Downloading [green]{object_name}[/]", total=obj_size
            )
            callback = RichProgressCallback(pbar, task_id)
            self.s3_resource.Bucket(bucket_name).download_file(
                object_name, dest_name, ExtraArgs=extraargs, Callback=callback
            )
