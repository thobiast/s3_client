#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sample python script to work with S3 buckets and objects.

This package performs basic S3 operations.
"""

import argparse
import functools
import logging
import os
import pprint
import sys
import time
from pathlib import Path

import boto3
import botocore
import tabulate
import tqdm
import urllib3


##############################################################################
# Parses the command line arguments
##############################################################################
def parse_parameters():
    """Command line parser."""
    # epilog message: Custom text after the help
    epilog = """
    Example of use:
        %(prog)s listbuckets
        %(prog)s --profile dev listbuckets
        %(prog)s -r us-east-1 listbuckets
        %(prog)s -e https://s3.amazonaws.com listobj my_bucket -t
        %(prog)s -e https://s3.amazonaws.com upload my_bucket -f file1
        %(prog)s -e https://s3.amazonaws.com upload my_bucket -d mydir
    """
    # Create the argparse object and define global options
    parser = argparse.ArgumentParser(
        description="S3 Client sample script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", dest="debug", help="debug flag"
    )
    parser.add_argument(
        "-e", "--endpoint", default=None, dest="endpoint", help="S3 endpoint URL"
    )
    parser.add_argument(
        "-r", "--region", default=None, dest="region_name", help="S3 Region Name"
    )
    parser.add_argument(
        "--profile", default=None, dest="aws_profile", help="AWS profile to use"
    )
    parser.add_argument(
        "--checksum-policy",
        dest="checksum_policy",
        default=None,
        choices=["when_supported", "when_required"],
        help=(
            "Apply checksum setting to both request and response. "
            "Valid: %(choices)s. (default: %(default)s)"
        ),
    )
    # Add subcommands options
    subparsers = parser.add_subparsers(title="Commands", dest="command")

    # List buckets
    listbuckets_parser = subparsers.add_parser("listbuckets", help="List all buckets")
    listbuckets_parser.add_argument(
        "--acl", default=False, action="store_true", help="Show ACL information"
    )
    listbuckets_parser.set_defaults(func=cmd_list_buckets)

    # List objects
    list_parser = subparsers.add_parser("listobj", help="List objects in a bucket")
    list_parser.add_argument(
        "-l",
        "--limit",
        type=int,
        required=False,
        default=None,
        help="Limit the number of objects returned",
    )
    list_parser.add_argument(
        "-t", "--table", action="store_true", help="Show output as table"
    )
    list_parser.add_argument(
        "-p", "--prefix", required=False, help="Only objects with specific prefix"
    )
    list_parser.add_argument(
        "-v", "--versions", action="store_true", help="Show all object versions"
    )
    list_parser.add_argument("bucket", help="Bucket Name")
    list_parser.set_defaults(func=cmd_list_obj)

    # Delete object
    deleteobj_parser = subparsers.add_parser(
        "deleteobj", help="Delete object in a bucket"
    )
    deleteobj_parser.add_argument("bucket", help="Bucket Name")
    deleteobj_parser.add_argument("object", help="Object Key Name")
    deleteobj_parser.add_argument(
        "-v",
        "--versionid",
        dest="versionid",
        help="""
        Object version id (in a versioning bucket this really delete the object version)
        """,
    )
    deleteobj_parser.set_defaults(func=cmd_delete_obj)

    # Metadata objects
    metadata_parser = subparsers.add_parser("metadataobj", help="List object metadata")
    metadata_parser.add_argument("bucket", help="Bucket Name")
    metadata_parser.add_argument("object", help="Object Key Name")
    metadata_parser.set_defaults(func=cmd_metadata_obj)

    # Upload file
    upload_parser = subparsers.add_parser("upload", help="Upload files to bucket")
    upload_parser.add_argument("bucket", help="Bucket Name")
    upload_parser.add_argument(
        "--nopbar",
        action="store_true",
        help="Disable progress bar",
    )
    upload_parser.add_argument(
        "--nokeepdir",
        default=False,
        action="store_true",
        help="Do not keep local directory structure on uploaded objects names",
    )
    upload_parser.add_argument(
        "-p",
        "--prefix",
        dest="prefix",
        default="",
        help="Prefix to add to the object name on upload",
    )
    upload_group = upload_parser.add_mutually_exclusive_group(required=True)
    upload_group.add_argument("-f", "--file", dest="filename", help="File to upload")
    upload_group.add_argument(
        "-d", "--dir", dest="dir", help="Directory to upload all files recursively"
    )
    upload_parser.set_defaults(func=cmd_upload)

    # Download file
    download_parser = subparsers.add_parser(
        "download", help="Download files from bucket"
    )
    download_parser.add_argument("bucket", help="Bucket Name")
    download_parser.add_argument(
        "--nopbar",
        action="store_true",
        help="Disable progress bar",
    )
    download_parser.add_argument(
        "-l",
        "--localdir",
        default=".",
        dest="localdir",
        help="Local directory to save downloaded file. Default current directory",
    )
    download_parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="Overwrite local destination file if it exists. Default false",
    )
    download_parser.add_argument(
        "-v",
        "--versionid",
        help="Object version id",
    )
    download_group = download_parser.add_mutually_exclusive_group(required=True)
    download_group.add_argument(
        "-f", "--file", dest="filename", help="Download a specific file"
    )
    download_group.add_argument(
        "-p",
        "--prefix",
        dest="prefix",
        help="Download recursively all files with a prefix.",
    )
    download_parser.set_defaults(func=cmd_download)

    # If there is no parameter, print help
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    return parser.parse_args()


def setup_logging(logfile=None, *, filemode="a", date_format=None, log_level="DEBUG"):
    """
    Configure logging.

    Arguments (opt):
        logfile     (str): log file to write the log messages
                               If not specified, it shows log messages
                               on screen (stderr)
    Keyword arguments (opt):
        filemode    (a/w): a - log messages are appended to the file (default)
                           w - log messages overwrite the file
        date_format (str): date format in strftime format
                           default is %m/%d/%Y %H:%M:%S
        log_level   (str): specifies the lowest-severity log message
                           DEBUG, INFO, WARNING, ERROR or CRITICAL
                           default is DEBUG
    """
    dict_level = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if log_level not in dict_level:
        raise ValueError("Invalid log_level")
    if filemode not in ["a", "w"]:
        raise ValueError("Invalid filemode")

    if not date_format:
        date_format = "%m/%d/%Y %H:%M:%S"

    log_fmt = "%(asctime)s %(module)s %(funcName)s %(levelname)s %(message)s"

    logging.basicConfig(
        level=dict_level[log_level],
        format=log_fmt,
        datefmt=date_format,
        filemode=filemode,
        filename=logfile,
    )

    return logging.getLogger(__name__)


def msg(color, msg_text, exitcode=0, *, end="\n", flush=True, output=None):
    """
    Print colored text.

    Arguments:
        color          (str): color name (blue, red, green, yellow,
                              cyan or nocolor)
        msg_text       (str): text to be printed
        exitcode  (int, opt): Optional parameter. If exitcode is different
                              from zero, it terminates the script, i.e,
                              it calls sys.exit with the exitcode informed

    Keyword arguments (optional):
        end            (str): string appended after the last char in "msg_text"
                              default a newline
        flush   (True/False): whether to forcibly flush the stream.
                              default True
        output      (stream): a file-like object (stream).
                              default sys.stdout

    Example:
        msg("blue", "nice text in blue")
        msg("red", "Error in my script. terminating", 1)
    """
    color_dic = {
        "blue": "\033[0;34m",
        "red": "\033[1;31m",
        "green": "\033[0;32m",
        "yellow": "\033[0;33m",
        "cyan": "\033[0;36m",
        "resetcolor": "\033[0m",
    }

    if not output:
        output = sys.stdout

    if not color or color == "nocolor":
        print(msg_text, end=end, file=output, flush=flush)
    else:
        if color not in color_dic:
            raise ValueError("Invalid color")
        print(
            "{}{}{}".format(color_dic[color], msg_text, color_dic["resetcolor"]),
            end=end,
            file=output,
            flush=flush,
        )

    if exitcode:
        sys.exit(exitcode)


def time_elapsed(func):
    """
    Calculate elapsed time in seconds.

    Decorator prints function elapsed time after its execution
    """

    @functools.wraps(func)
    def wrapped_f(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        # keep track of total elapsed time for all execution of the function
        wrapped_f.elapsed += elapsed_time

        output = "  - Elapsed time {:.4f} seconds".format(elapsed_time)
        msg("nocolor", output)

        return result

    wrapped_f.elapsed = 0
    return wrapped_f


class Config:
    """
    Handles configuration for AWS services by initializing a boto3 session.

    This class supports initializing configurations using either an AWS profile
    or environment variables. If an AWS profile is specified, it attempts to use
    that profile to create a boto3 session. If no profile is specified, it falls back
    to using credentials specified in environment variables.

    Attributes:
        session (boto3.Session): A boto3 Session object initialized.
        region_name (str): The AWS region name.
        s3_endpoint (str): The custom S3 endpoint URL.
    """

    def __init__(self, profile_name=None, region_name=None, s3_endpoint=None):
        """
        Initialize configurations using AWS profile or environment variables.
        If a profile name is provided, it will use that profile.
        Otherwise, it checks for environment variables and raises an error if they are not set.

        Params:
            profile_name (str, optional): The name of the AWS profile to use.
            region_name (str, optional): The AWS region name to use.
            s3_endpoint (str, optional): The custom S3 endpoint URL.
        """
        self.region_name = region_name
        self.s3_endpoint = s3_endpoint

        if profile_name:
            self.session = boto3.Session(
                profile_name=profile_name, region_name=region_name
            )
            try:
                if not self.session.get_credentials():
                    raise ValueError(
                        f"Could not find credentials for AWS profile '{profile_name}'"
                    )
            except botocore.exceptions.PartialCredentialsError as e:
                raise ValueError(
                    f"Partial credentials found for AWS profile '{profile_name}'. {e}"
                )
        else:
            self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            self.aws_session_token = os.getenv("AWS_SESSION_TOKEN")  # Optional

            if not self.aws_access_key or not self.aws_secret_key:
                raise ValueError(
                    "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in environment variables."
                )

            self.session = boto3.Session(
                region_name=region_name,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token,
            )

    def get_session(self):
        """
        Returns the boto3 session.

        Returns:
            boto3.Session: The initialized boto3 session.
        """
        return self.session


class ProgressBar(tqdm.tqdm):
    """Class to display progress bar."""

    def update_to(self, bytes_sent):
        """
        Update tqdm status bar.

        Params:
            bytes_sent    (int): number of bytes transferred
        """
        return self.update(bytes_sent)


class S3:
    """
    Class to handle S3 operations.

    Attributes:
        s3_resource (boto3.resource): The boto3 S3 resource object used to interact with S3.
        disable_pbar (bool): Flag to disable the progress bar display.
        buckets_exist (list): A list to cache bucket existence checks.
    """

    def __init__(self, config, checksum_policy=None):
        """
        Initializes the S3 manager with configurations provided by the Config object.

        Params:
            config (Config): The configuration object providing the session,
                             region, and S3 endpoint information.
        """
        boto3_session = config.get_session()

        boto3_config = botocore.config.Config(
            request_checksum_calculation=checksum_policy,
            response_checksum_validation=checksum_policy,
        )

        self.s3_resource = boto3_session.resource(
            "s3",
            endpoint_url=config.s3_endpoint,
            region_name=config.region_name,
            config=boto3_config,
        )
        self.disable_pbar = False
        self.buckets_exist = []

    def check_bucket_exist(self, bucket_name):
        """
        Checks if the specified bucket exists and caches the result.

        Params:
            bucket_name           (str): Bucket name

        Return:
            bool: True if the bucket exists, False otherwise.
        """
        # If bucket was already checked, return it exists
        if bucket_name in self.buckets_exist:
            log.debug("bucket %s was already checked, do not check again", bucket_name)
            return True

        try:
            log.debug("Checking if bucket exist: %s", bucket_name)
            self.s3_resource.meta.client.head_bucket(Bucket=bucket_name)
            self.buckets_exist.append(bucket_name)
            return True
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def check_bucket_versioning(self, bucket_name):
        """
        Return bucket versioning status.

        Params:
            bucket_name           (str): Bucket name

        Return:
            (str)
        """
        return self.s3_resource.BucketVersioning(bucket_name).status

    def list_buckets(self):
        """
        List all buckets.

        Returns:
            A list of Bucket resources
        """
        return self.s3_resource.buckets.all()

    def list_objects(self, bucket_name, *, prefix=None, limit=None):
        """
        List objects stored in a bucket.

        Params:
            bucket_name      (str): Bucket name

        Keyword arguments (opt):
            prefix           (str): Filter only objects with specific prefix
                                    default None
            limit            (int): Limit the number of objects returned
                                    default None

        Returns:
            An iterable of ObjectSummary resources
        """
        if prefix:
            return (
                self.s3_resource.Bucket(bucket_name)
                .objects.filter(
                    Prefix=prefix,
                )
                .limit(limit)
            )
        else:
            return self.s3_resource.Bucket(bucket_name).objects.all().limit(limit)

    def list_objects_versions(self, bucket_name, *, prefix=None, limit=None):
        """
        List all objects versions stored in a bucket.

        Params:
            bucket_name      (str): Bucket name

        Keyword arguments (opt):
            prefix           (str): Filter only objects with specific prefix
                                    default None
            limit            (int): Limit the number of objects returned
                                    default None

        Returns:
            An iterable of ObjectVersion resources
        """
        if prefix:
            return (
                self.s3_resource.Bucket(bucket_name)
                .object_versions.filter(
                    Prefix=prefix,
                )
                .limit(limit)
            )
        else:
            return (
                self.s3_resource.Bucket(bucket_name).object_versions.all().limit(limit)
            )

    def metadata_object(self, bucket_name, object_name):
        """
        Return object metadata.

        Params:
            bucket_name           (str): Bucket name
            object_name           (str): Object key name
        """
        return self.s3_resource.meta.client.head_object(
            Bucket=bucket_name, Key=object_name
        )

    def delete_object(self, bucket_name, object_name, version_id=None):
        """
        Delete an object.

        Params:
            bucket_name           (str): Bucket name
            object_name           (str): Object key name
            version_id            (str): Object version id
        """
        obj = {"Key": object_name}
        if version_id:
            obj["VersionId"] = version_id

        return self.s3_resource.Bucket(bucket_name).delete_objects(
            Delete={"Objects": [obj]}
        )

    @time_elapsed
    def upload_file(self, bucket_name, file_name, key_name=None):
        """
        Upload a file from local source to S3.

        Params:
            bucket_name        (str): The name of the bucket to upload to
            file_name          (str): The path to the file to upload
            key_name           (str): The name of the key to upload to
                                      If key_name is None, the file_name
                                      is used as object name
        """
        if key_name is None:
            key_name = file_name

        log.debug("Uploading file: %s with key: %s", file_name, key_name)

        obj_size = Path(file_name).stat().st_size
        with ProgressBar(
            unit="B",
            unit_scale=True,
            desc="data transferred",
            total=obj_size,
            miniters=1,
            disable=self.disable_pbar,
        ) as pbar:
            self.s3_resource.Bucket(bucket_name).upload_file(
                Filename=file_name,
                Key=key_name,
                Callback=pbar.update_to,
            )

    @time_elapsed
    def download_object(self, bucket_name, object_name, dest_name, versionid=None):
        """
        Download an object from S3 to local source.

        Params:
            bucket_name            (str): Bucket name
            object_name            (str): Object name
            dest_name              (str): Full path filename to store the object
            versionid             (str): Object version id
        """
        log.debug("Downloading object %s to dest %s", object_name, dest_name)

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

        log.debug("obj_size: %s, extraargs: %s", obj_size, extraargs)
        with ProgressBar(
            unit="B",
            unit_scale=True,
            desc="data transferred",
            total=obj_size,
            miniters=1,
            disable=self.disable_pbar,
        ) as pbar:
            self.s3_resource.Bucket(bucket_name).download_file(
                object_name, dest_name, ExtraArgs=extraargs, Callback=pbar.update_to
            )


class Download:
    """Class to download files."""

    def __init__(self, s3, bucket_name, local_dir):
        """
        Initialize Download class.

        Params:
            s3           (obj): Instance of S3 class
            bucket_name  (str): Bucket name
            local_dir    (str): Local directory to save downloaded files
        """
        self.s3 = s3
        self.bucket_name = bucket_name
        self.local_dir = Path(local_dir)

    def download_file(self, object_name, overwrite, versionid=None):
        """
        Download a file from S3.

        Params:
            object_name          (str): Object name to download
            overwrite     (True/False): Overwrite local file if it already exists
            versionid            (str): Object version id
        """
        # Stripping leading slashes to ensure relative join
        safe_object_name = object_name.lstrip("/")
        # Set full file path to store the object
        dest_path = self.local_dir.joinpath(safe_object_name)

        # Check if destination file already exists and overwrite is false
        if not overwrite and dest_path.is_file():
            msg(
                "red",
                f"Error: File {dest_path} exists. Use --overwrite to replace it.",
                1,
            )
        # Create parent directories if they don't exist
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            msg(
                "red",
                f"Error: Permission denied to create directory {dest_path.parent}",
                1,
            )

        msg("cyan", f"Downloading object {object_name} to path {dest_path}")

        try:
            self.s3.download_object(
                self.bucket_name, object_name, str(dest_path), versionid
            )
        except PermissionError:
            msg("red", f"Error: Permission denied to write file {dest_path}", 1)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                msg("red", f"Error:  object '{object_name}' not found.", 1)
            else:
                raise

        msg("green", "  - Download completed successfully")

    def download_prefix(self, prefix, overwrite):
        """
        Download files that start with a prefix from S3.

        Params:
            prefix             (str): Object prefix name
            overwrite   (True/False): Overwrite local file if it already exist
        """
        for obj in self.s3.list_objects(self.bucket_name, prefix=prefix):
            self.download_file(obj.key, overwrite)


##############################################################################
# Command to list object metadata
##############################################################################
def cmd_metadata_obj(s3, args):
    """Handle metadataobj option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", "Error: Bucket '{}' does not exist".format(args.bucket), 1)

    try:
        pprint.pprint(s3.metadata_object(args.bucket, args.object))
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "404":
            msg("red", "Error: key '{}' not found".format(args.object), 1)
        else:
            raise


##############################################################################
# Delete object
##############################################################################
def cmd_delete_obj(s3, args):
    """Handle delete object option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", "Error: Bucket '{}' does not exist".format(args.bucket), 1)

    resp = s3.delete_object(args.bucket, args.object, args.versionid)
    pprint.pprint(resp["Deleted"])


##############################################################################
# Command to list all buckets
##############################################################################
def cmd_list_buckets(s3, args):
    """Handle listbuckets option."""
    attrs = ["name", "creation_date"]
    for bucket in s3.list_buckets():
        for attr in attrs:
            msg("cyan", attr, end=": ")
            msg("nocolor", getattr(bucket, attr), end=" ")
        versioning = s3.check_bucket_versioning(bucket.name)
        msg("cyan", "versioning_status", end=": ")
        msg("nocolor", versioning)
        if args.acl:
            msg("cyan", "  acl: ")
            msg("nocolor", "   {}".format(pprint.pformat(bucket.Acl().grants)))


##############################################################################
# Command to list all bucket's objects
##############################################################################
def cmd_list_obj(s3, args):
    """Handle listobj option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", "Error: Bucket '{}' does not exist".format(args.bucket), 1)

    if args.versions:
        objects = s3.list_objects_versions(
            args.bucket, prefix=args.prefix, limit=args.limit
        )
        # Resource's attributes:
        attrs = [
            "key",
            "size",
            "storage_class",
            "e_tag",
            "last_modified",
            "version_id",
            "is_latest",
        ]
    else:
        objects = s3.list_objects(args.bucket, prefix=args.prefix, limit=args.limit)
        # Resource's attributes:
        attrs = ["key", "size", "storage_class", "e_tag", "last_modified"]

    if args.table:
        # Tabulate needs to keep the entire table in-memory
        table = []
        # Use the first row of data as a table header
        table.append(attrs)
        for obj in objects:
            line = [getattr(obj, attr) for attr in attrs]
            table.append(line)
        print(tabulate.tabulate(table, headers="firstrow", tablefmt="github"))
    else:
        for obj in objects:
            for attr in attrs:
                msg("cyan", attr, end=": ")
                msg("nocolor", getattr(obj, attr), end=" ")
            msg("nocolor", "")


##############################################################################
# Upload a file to S3
##############################################################################
def upload_file_to_s3(s3, bucket_name, file_path, object_name):
    """
    Upload a single file to an S3 bucket.

    Parameters:
        s3 (S3): An instance of the S3 class.
        bucket_name (str): The name of the S3 bucket where the file will be uploaded.
        file_path (str): The path of the file on the local file system to upload.
        object_name (str): The target object name in the S3 bucket. This is the name
                           that will be used to store the file in the bucket.
    """

    msg("cyan", f"Uploading file {file_path} with object name {object_name}")

    try:
        s3.upload_file(bucket_name, file_path, object_name)
    except PermissionError:
        msg("red", f"Error: permission denied to read file {file_path}", 1)
    except FileNotFoundError:
        msg("red", f"Error: File '{file_path}' not found", 1)

    msg("green", "  - Upload completed successfully")


def upload_construct_object_name(file_path_str, prefix, nokeepdir):
    """
    Construct the object name for S3 upload, considering prefix and directory structure.

    Args:
        file_path_str (str): The path to the file being uploaded.
        prefix (str): The prefix string to prepend to the object name.
        nokeepdir (bool): Flag to keep or discard the directory structure in the object name.
    """
    file_path = Path(file_path_str)
    # Extract the base filename if nokeepdir is set; otherwise, use the full file_path
    base_name = file_path.name if nokeepdir else file_path_str

    # Add the prefix to the object_name
    return f"{prefix}{base_name}"


##############################################################################
# Command to upload file or directory
##############################################################################
def cmd_upload(s3, args):
    """
    Command to upload files or directories to an S3 bucket.

    This function handles the 'upload' command line argument. It uploads
    either a single file or all files within a directory to the specified S3 bucket.

    Args:
        s3 (S3): An instance of the S3 class.
        args (argparse.Namespace): Command line arguments.
    """
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    s3.disable_pbar = args.nopbar

    if args.filename:
        file_path = Path(args.filename)
        if file_path.is_file():
            object_name = upload_construct_object_name(
                str(file_path), args.prefix, args.nokeepdir
            )
            upload_file_to_s3(s3, args.bucket, str(file_path), object_name)
        else:
            msg(
                "red",
                f"Error: The specified file '{args.filename}' does not exist or is not a file.",
                1,
            )

    if args.dir:
        dir_path = Path(args.dir)
        if not dir_path.is_dir():
            msg("red", f"Error: Directory '{dir_path}' not found", 1)
        # Use os.walk for Python <3.12 compatibility. Migrate to Path.walk() later
        for dirpath, _dirnames, files in os.walk(args.dir):
            for filename in files:
                full_path = Path(dirpath).joinpath(filename)
                object_name = upload_construct_object_name(
                    str(full_path), args.prefix, args.nokeepdir
                )
                upload_file_to_s3(s3, args.bucket, str(full_path), object_name)


##############################################################################
# Command to download objects
##############################################################################
def cmd_download(s3, args):
    """Handle download option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    s3.disable_pbar = args.nopbar

    # Check if local directory exists
    local_path = Path(args.localdir)
    if not local_path.is_dir():
        msg("red", f"Error: local path '{local_path}' is not a valid directory", 1)

    download = Download(s3, args.bucket, args.localdir)

    # Download a specific object
    if args.filename:
        download.download_file(args.filename, args.overwrite, args.versionid)

    # Download all objects with a prefix
    if args.prefix:
        download.download_prefix(args.prefix, args.overwrite)


##############################################################################
# Main function
##############################################################################
def main():
    """Command line execution."""
    global log

    # Parser the command line
    args = parse_parameters()

    # By default some modules write log messages to console.
    # The following line configure it to only write messages if is
    # at least error
    logging.getLogger("boto3").setLevel(logging.ERROR)
    logging.getLogger("s3transfer").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    urllib3.disable_warnings()
    # Configure log if --debug
    log = setup_logging() if args.debug else logging
    log.debug("CMD line args: %s", vars(args))

    try:
        config = Config(
            profile_name=args.aws_profile,
            region_name=args.region_name,
            s3_endpoint=args.endpoint,
        )
    except ValueError as error:
        msg("red", str(error), 1)

    s3 = S3(config, checksum_policy=args.checksum_policy)

    # Execute the function (command)
    if args.command is not None:
        args.func(s3, args)


##############################################################################
# Run from command line
##############################################################################
if __name__ == "__main__":
    main()

# vim: ts=4
