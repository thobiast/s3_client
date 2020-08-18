#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sample python script to work with S3 buckets and objects.

This package performs basic s3 operation.
"""

import argparse
import datetime
import logging
import os
import pprint
import sys

import boto3

import botocore

import tabulate

import urllib3


##############################################################################
# Parses the command line arguments
##############################################################################
def parse_parameters():
    """Command line parser."""
    # epilog message: Custom text after the help
    epilog = """
    Example of use:
        %s -e https://s3.amazonaws.com listbuckets
        %s -e https://s3.amazonaws.com listobj my_bucket -t
        %s -e https://s3.amazonaws.com upload my_bucket -f file1
        %s -e https://s3.amazonaws.com upload my_bucket -d mydir
    """ % (
        sys.argv[0],
        sys.argv[0],
        sys.argv[0],
        sys.argv[0],
    )
    # Create the argparse object and define global options
    parser = argparse.ArgumentParser(
        description="S3 Client sample script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "--debug", "-d", action="store_true", dest="debug", help="debug flag"
    )
    parser.add_argument(
        "--endpoint", "-e", default=None, dest="endpoint", help="S3 endpoint URL"
    )
    # Add subcommands options
    subparsers = parser.add_subparsers(title="Commands", dest="command")

    # list buckets
    listbuckets_parser = subparsers.add_parser("listbuckets", help="List all buckets")
    listbuckets_parser.add_argument(
        "--acl", default=False, action="store_true", help="Show ACL information"
    )
    listbuckets_parser.set_defaults(func=cmd_list_buckets)

    # List objects
    list_parser = subparsers.add_parser("listobj", help="List Objects in a bucket")
    list_parser.add_argument(
        "--table", "-t", action="store_true", help="Show output as table"
    )
    list_parser.add_argument(
        "--prefix", "-p", required=False, help="Only objects with specificrefix"
    )
    list_parser.add_argument("bucket", help="Bucket Name")
    list_parser.set_defaults(func=cmd_list_obj)

    # Metadata objects
    metadata_parser = subparsers.add_parser("metadataobj", help="List Objects Metadata")
    metadata_parser.add_argument("bucket", help="Bucket Name")
    metadata_parser.add_argument("object", help="Object Key Name")
    metadata_parser.set_defaults(func=cmd_metadata_obj)

    # Upload file
    upload_parser = subparsers.add_parser("upload", help="Upload files to bucket")
    upload_parser.add_argument("bucket", help="Bucket Name")
    upload_parser.add_argument(
        "--nokeepdir",
        default=False,
        action="store_true",
        help="Do not keep local directory structure \
                                    on uploaded objects names",
    )
    upload_group = upload_parser.add_mutually_exclusive_group(required=True)
    upload_group.add_argument("--file", "-f", dest="filename", help="File to upload")
    upload_group.add_argument(
        "--dir", "-d", dest="dir", help="Directory to upload all files recursively"
    )
    upload_parser.set_defaults(func=cmd_upload)

    # Download file
    download_parser = subparsers.add_parser(
        "download", help="Download files from bucket"
    )
    download_parser.add_argument("bucket", help="Bucket Name")
    download_parser.add_argument(
        "--localdir",
        "-l",
        default=".",
        dest="localdir",
        help="Local directory to save downloaded " "file. Default current directory",
    )
    download_group = download_parser.add_mutually_exclusive_group(required=True)
    download_group.add_argument(
        "--file", "-f", dest="filename", help="Download a specific file"
    )
    download_group.add_argument(
        "--prefix",
        "-p",
        dest="prefix",
        help="Download recursively all files " "with a prefix.",
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


def msg(color, msg_text, exitcode=0, *, end="\n"):
    """
    Print colored text.

    Arguments:
        size      (str): color name (blue, red, green, yellow,
                                     cyan or nocolor)
        msg_text  (str): text to be printed
        exitcode  (int, opt): Optional parameter. If exitcode is different
                              from zero, it terminates the script, i.e,
                              it calls sys.exit with the exitcode informed

    Keyword arguments:
        end         (str, opt): string appended after the last value,
                                default a newline


    Exemplo:
        msg("blue", "nice text in blue")
        msg("red", "Error in my script.. terminating", 1)
    """
    color_dic = {
        "blue": "\033[0;34m",
        "red": "\033[1;31m",
        "green": "\033[0;32m",
        "yellow": "\033[0;33m",
        "cyan": "\033[0;36m",
        "resetcolor": "\033[0m",
    }

    if not color or color == "nocolor":
        print(msg_text, end=end)
    else:
        try:
            print(color_dic[color] + msg_text + color_dic["resetcolor"], end=end)
        except KeyError as exc:
            raise ValueError("Invalid color") from exc

    # flush stdout
    sys.stdout.flush()

    if exitcode:
        sys.exit(exitcode)


def create_dir(local_path):
    """
    Create a local directory. It supports nested directory.

    Params:
        local_path   (str): Directory to create
    """
    # Check if local_path already exist
    if os.path.exists(local_path):
        if os.path.isfile(local_path):
            msg(
                "red",
                "Error: path {} exists and is not a directory".format(local_path),
                1,
            )
    else:
        try:
            os.makedirs(local_path)
        except PermissionError:
            msg("red", "Error: PermissionError to create dir {}".format(local_path), 1)


class Config:
    """Class to handle configurations."""

    def __init__(self):
        """Initialize configurations."""
        self.aws_access_key_id = self.get_env("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = self.get_env("AWS_SECRET_ACCESS_KEY")

    @staticmethod
    def get_env(var):
        """Read environment variable."""
        if not os.environ.get(var):
            raise ValueError(
                "Error: You must export environment variable {}".format(var)
            )
        return os.environ.get(var)


class S3:
    """Class to handle S3 operations."""

    def __init__(self, key, secret, s3_endpoint):
        """
        Initialize s3 class.

        Params:
            key           (str): AWS_ACCESS_KEY_ID
            secret        (str): AWS_SECRET_ACCESS_KEY
            s3_endpoint   (str): S3 endpoint URL
        """
        self.s3_resource = boto3.resource(
            "s3",
            endpoint_url=s3_endpoint,
            verify=False,
            aws_access_key_id=key,
            aws_secret_access_key=secret,
        )
        self.buckets_exist = []

    def check_bucket_exist(self, bucket_name):
        """
        Verify if a bucket exists.

        Params:
            bucket_name           (str): Bucket name

        Return:
            1     : bucket exists
            0     : bucket does not exist
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

    def list_buckets(self):
        """
        List all buckets.

        Returns:
            A list of Bucket resources
        """
        return self.s3_resource.buckets.all()

    def list_objects(self, bucket_name, prefix=None):
        """
        List objects stored in a bucket.

        Params:
            bucket_name      (str): Bucket name
            prefix           (str): Filter only objects with specific prefix
                                    default None

        Returns:
            An iterable of ObjectSummary resources
        """
        if prefix:
            return self.s3_resource.Bucket(bucket_name).objects.filter(
                Delimiter="/", Prefix=prefix,
            )
        else:
            return self.s3_resource.Bucket(bucket_name).objects.all()

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

    def upload_file(self, bucket_name, file_name, keep_structure=True):
        """
        Upload a file from local source to S3.

        Params:
            bucket_name           (str): Bucket name
            file_name             (str): File name
            keep_structure (True/False): Keep original directory structure in
                                         key_name. (default: True)
        """
        log.debug("Uploading file: %s", file_name)
        if not os.path.isfile(file_name):
            msg("red", "Error: File '{}' not found".format(file_name), 1)

        key_name = file_name if keep_structure else file_name.split("/")[-1]

        msg("cyan", "Uploading file {} with key {}".format(file_name, key_name), end="")
        start_time = datetime.datetime.now()
        self.s3_resource.Bucket(bucket_name).upload_file(
            Filename=file_name, Key=key_name
        )
        elapsed_time = datetime.datetime.now() - start_time
        msg("green", " Done. Elapsed time (hh:mm:ss.mm) {}".format(elapsed_time))

    def upload_dir(self, bucket_name, dir_name, keep_structure=True):
        """
        Upload all files recursively from local directory to S3.

        Params:
            bucket_name           (str): Bucket name
            dir_name              (str): Directory name
            keep_structure (True/False): Keep original directory structure in
                                         key_name. (default: True)
        """
        if not os.path.isdir(dir_name):
            msg("red", "Error: Directory '{}' not found".format(dir_name), 1)

        for dirpath, _dirnames, files in os.walk(dir_name):
            for filename in files:
                object_name = os.path.join(dirpath, filename)
                self.upload_file(
                    bucket_name, object_name, keep_structure=keep_structure
                )

    def download_file(self, bucket_name, object_name, *, local_dir="."):
        """
        Download an object from S3 to local directory.

        Params:
            bucket_name            (str): Bucket name
            object_name            (str): Object name (file name)

        Keyword arguments:
            local_dir              (str): Local directory to save downloaded
                                          files. Default: Current directory
        """
        log.debug("local_dir: %s", local_dir)
        log.debug("object_name: %s", object_name)
        if not local_dir.endswith("/") and not object_name.startswith("/"):
            dest_name = local_dir + "/" + object_name
        elif local_dir.endswith("/") and object_name.startswith("/"):
            dest_name = local_dir + object_name[1:]
        else:
            dest_name = local_dir + object_name

        log.debug("dest_name: %s", dest_name)
        # Check if file exist on local drive
        if os.path.isfile(dest_name):
            msg(
                "red",
                "Erro: File {} exist. Remove it from local drive to download.".format(
                    dest_name
                ),
                1,
            )

        # If necessary, create directories structure to save
        # the downloaded file
        local_path = "/".join(dest_name.split("/")[:-1])
        create_dir(local_path)

        msg("cyan", "Downloading {} to dir '{}'".format(object_name, local_dir), end="")
        try:
            self.s3_resource.Bucket(bucket_name).download_file(object_name, dest_name)
            msg("green", " Done.")
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "404":
                msg("red", " The object '{}' does not exist.".format(object_name))
            else:
                raise

    def download_prefix(self, bucket_name, prefix, *, local_dir="."):
        """
        Download (recursively) all objects with a prefix from S3 to local directory.

        Params:
            bucket_name           (str): Bucket name
            prefix                (str): Prefix of files to download

        Keyword arguments:
            local_dir             (str): Local directory to save downloaded
                                         files. Default: Current directory
        """
        msg(
            "blue",
            "Downloading files prefix {} from bucket {} to local dir {}".format(
                prefix, bucket_name, local_dir
            ),
        )

        for obj in self.s3_resource.Bucket(bucket_name).objects.filter(Prefix=prefix):
            self.download_file(bucket_name, obj.key, local_dir=local_dir)


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
# Command to list all buckets
##############################################################################
def cmd_list_buckets(s3, args):
    """Handle listbuckets option."""
    attrs = ["name", "creation_date"]
    for bucket in s3.list_buckets():
        for attr in attrs:
            msg("cyan", attr, end=": ")
            msg("nocolor", getattr(bucket, attr), end=" ")
        msg("nocolor", "")
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

    objects = s3.list_objects(args.bucket, args.prefix)

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
# Command to upload file or directory
##############################################################################
def cmd_upload(s3, args):
    """Handle upload option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", "Error: Bucket '{}' does not exist".format(args.bucket), 1)

    # If args.keepdir was specified at command line, then set
    # keep_structure to false, otherwise, set to True
    keep_structure = not args.nokeepdir

    if args.filename:
        s3.upload_file(args.bucket, args.filename, keep_structure=keep_structure)

    if args.dir:
        s3.upload_dir(args.bucket, args.dir, keep_structure=keep_structure)


##############################################################################
# Command to download objects
##############################################################################
def cmd_download(s3, args):
    """Handle download option."""
    # Check if bucket exist
    if not s3.check_bucket_exist(args.bucket):
        msg("red", "Error: Bucket '{}' does not exist".format(args.bucket), 1)

    # Download a specific object
    if args.filename:
        s3.download_file(args.bucket, args.filename, local_dir=args.localdir)
    # Download all objects with a prefix
    if args.prefix:
        s3.download_prefix(args.bucket, args.prefix, local_dir=args.localdir)


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
        config = Config()
    except ValueError as error:
        msg("red", str(error), 1)

    s3 = S3(config.aws_access_key_id, config.aws_secret_access_key, args.endpoint,)

    # Execute the funcion (command)
    if args.command is not None:
        args.func(s3, args)


##############################################################################
# Run from command line
##############################################################################
if __name__ == "__main__":
    main()

# vim: ts=4
