#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sample python script to work with S3 buckets and objects.

This package performs basic S3 operations.
"""

import argparse
import logging
import os
import pprint
import sys
from pathlib import Path

import botocore
import urllib3
from rich.console import Console
from rich.table import Table

from .s3_core import Config, S3Manager
from .utils import bytes2human, msg, setup_logging


##############################################################################
# Parses the command line arguments
##############################################################################
# pylint: disable=too-many-statements
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

    # Create bucket
    createbucket_parser = subparsers.add_parser(
        "createbucket", help="Create a new bucket"
    )
    createbucket_parser.add_argument("bucket", help="Bucket Name to create")
    createbucket_parser.add_argument(
        "-v",
        "--versioned",
        action="store_true",
        help="Enable versioning on the new bucket (default: %(default)s)",
    )
    createbucket_parser.set_defaults(func=cmd_create_bucket)

    # List buckets
    listbuckets_parser = subparsers.add_parser("listbuckets", help="List all buckets")
    listbuckets_parser.add_argument(
        "--acl", default=False, action="store_true", help="Show ACL information"
    )
    listbuckets_parser.set_defaults(func=cmd_list_buckets)

    # Delete bucket
    deletebucket_parser = subparsers.add_parser(
        "deletebucket", help="Delete an empty S3 bucket"
    )
    deletebucket_parser.add_argument("bucket", help="Bucket Name to delete")
    deletebucket_parser.set_defaults(func=cmd_delete_bucket)

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
        Object version ID (in a versioning bucket this really delete the object version)
        """,
    )
    deleteobj_parser.set_defaults(func=cmd_delete_obj)

    # Metadata objects
    metadata_parser = subparsers.add_parser("metadataobj", help="List object metadata")
    metadata_parser.add_argument("bucket", help="Bucket Name")
    metadata_parser.add_argument("object", help="Object Key Name")
    metadata_parser.add_argument(
        "-v",
        "--versionid",
        dest="versionid",
        help="Object version ID to retrieve metadata",
    )
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
        help="Object version ID",
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


##############################################################################
# Command to list object metadata
##############################################################################
def cmd_metadata_obj(s3manager, args):
    """Handle metadataobj option."""
    # Check if bucket exists
    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    try:
        pprint.pprint(
            s3manager.metadata_object(args.bucket, args.object, args.versionid)
        )
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "404":
            if args.versionid:
                msg(
                    "red",
                    f"Error: key '{args.object}' with version "
                    f"'{args.versionid}' not found",
                    1,
                )
            else:
                msg("red", f"Error: key '{args.object}' not found", 1)
        else:
            raise


##############################################################################
# Delete object
##############################################################################
def cmd_delete_obj(s3manager, args):
    """Handle delete object option."""
    # Check if bucket exists
    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    resp = s3manager.delete_object(args.bucket, args.object, args.versionid)
    pprint.pprint(resp)


##############################################################################
# Command to create a bucket
##############################################################################
def cmd_create_bucket(s3manager, args):
    """Handle createbucket option."""
    msg("cyan", f"Attempting to create bucket '{args.bucket}'...")

    if s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' already exists", 1)

    try:
        s3manager.create_bucket(args.bucket, args.versioned)

        msg("green", f"Successfully created bucket '{args.bucket}'")
        if args.versioned:
            msg("green", f"  - Versioning enabled for '{args.bucket}'")

    except botocore.exceptions.ClientError as error:
        msg("red", f"Error creating bucket: {error}", 1)
    except Exception as error:
        msg("red", f"An unexpected error occurred: {error}", 1)


##############################################################################
# Command to delete a bucket
##############################################################################
def cmd_delete_bucket(s3manager, args):
    """Handle deletebucket option."""

    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' not found", 1)

    # --- confirmation
    msg("red", f"!!! WARNING: This will permanently delete the bucket '{args.bucket}'")
    msg("red", "The bucket must be empty to be deleted")
    msg("yellow", f"To confirm, please type the bucket name ('{args.bucket}') again:")

    try:
        confirmation = input("> ")
    except (EOFError, KeyboardInterrupt):
        msg("cyan", "\nDeletion cancelled.", 1)
        return

    if confirmation != args.bucket:
        msg("red", "Bucket name confirmation mismatch. Bucket deletion cancelled.", 1)
        return
    # --- end confirmation ---

    msg("cyan", f"Proceeding with deletion of '{args.bucket}'...")

    try:
        s3manager.delete_bucket(args.bucket)
        msg("green", f"Successfully deleted bucket '{args.bucket}'")

    except botocore.exceptions.ClientError as error:
        error_code = error.response["Error"]["Code"]
        if error_code == "BucketNotEmpty":
            msg("red", f"Error: Bucket '{args.bucket}' is not empty", 1)
        elif error_code == "NoSuchBucket":
            msg("red", f"Error: Bucket '{args.bucket}' not found", 1)
        else:
            msg("red", f"Error deleting bucket: {error}", 1)
    except Exception as e:
        msg("red", f"An unexpected error occurred: {e}", 1)


##############################################################################
# Command to list all buckets
##############################################################################
def cmd_list_buckets(s3manager, args):
    """Handle listbuckets option."""
    attrs = ["name", "creation_date"]
    for bucket in s3manager.list_buckets():
        for attr in attrs:
            msg("cyan", attr, end=": ")
            msg("nocolor", getattr(bucket, attr), end=" ")
        versioning = s3manager.check_bucket_versioning(bucket.name)
        msg("cyan", "versioning_status", end=": ")
        msg("nocolor", versioning)
        if args.acl:
            msg("cyan", "  acl: ")
            msg("nocolor", f"   {pprint.pformat(bucket.Acl().grants)}")


def print_objects_table(objects, bucket_name, attrs):
    """
    Helper function to print list of objects as a rich Table.

    Arguments:
        objects     (iterable): Collection of S3 objects (ObjectSummary or
                                ObjectVersion) to display.
        bucket_name (str):      Name of the S3 bucket.
        attrs       (list):     List of object attributes to display as columns.
                                Example: ["key", "size", "storage_class", "e_tag"]
    """
    column_definitions = {
        "key": ("Key", {"justify": "left", "style": "cyan", "overflow": "fold"}),
        "size": ("Size", {"justify": "right", "style": "green", "no_wrap": True}),
        "storage_class": ("Storage Class", {"justify": "left"}),
        "e_tag": ("ETag", {"justify": "left", "style": "dim", "no_wrap": True}),
        "last_modified": ("Last Modified", {"justify": "left", "style": "white"}),
        "version_id": (
            "Version ID",
            {"justify": "left", "style": "dim", "no_wrap": True},
        ),
        "is_latest": ("Is Latest", {"justify": "center", "no_wrap": True}),
    }
    table = Table(
        title=f"Objects in S3 Bucket {bucket_name}",
        title_style="bold magenta",
        header_style="bold",
        show_lines=False,
        pad_edge=False,
        caption_style="dim",
    )
    for attr in attrs:
        # Get the display name and style options
        display_name, options = column_definitions.get(attr, (attr.capitalize(), {}))
        table.add_column(display_name, **options)

    total_size = 0
    for obj in objects:
        row_data = [str(getattr(obj, attr, "N/A")) for attr in attrs]
        table.add_row(*row_data)
        size = getattr(obj, "size", None)
        total_size += size or 0

    readable_size = " ".join(bytes2human(total_size))
    table.caption = f"{table.row_count} object(s)  •  total size {readable_size}"
    console = Console()
    console.print(table)


def print_objects_lines(objects, attrs):
    """
    Helper function to print list of objects as simple lines.

    Arguments:
        objects      (iterable): Collection of S3 objects (ObjectSummary or
                                 ObjectVersion) to display.
        attrs        (list): List of object attributes to print for each object.
                             Example: ["key", "size", "storage_class", "e_tag"]
    """
    for obj in objects:
        for attr in attrs:
            msg("cyan", attr, end=": ")
            msg("nocolor", getattr(obj, attr), end=" ")
        msg("nocolor", "")


##############################################################################
# Command to list all bucket's objects
##############################################################################
def cmd_list_obj(s3manager, args):
    """Handle listobj option."""
    # Check if bucket exists
    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    # Resource's attributes:
    base_attrs = ["key", "size", "storage_class", "e_tag", "last_modified"]

    if args.versions:
        objects = s3manager.list_objects_versions(
            args.bucket, prefix=args.prefix, limit=args.limit
        )
        # Add version information to resource's attributes
        attrs = base_attrs + ["version_id", "is_latest"]
    else:
        objects = s3manager.list_objects(
            args.bucket, prefix=args.prefix, limit=args.limit
        )
        attrs = base_attrs

    if args.table:
        print_objects_table(objects, args.bucket, attrs)
    else:
        print_objects_lines(objects, attrs)


def upload_construct_object_name(file_path_str, prefix, nokeepdir):
    """
    Construct the object name for S3 upload, considering prefix and directory structure.

    Arguments:
        file_path_str (str):  The path to the file being uploaded.
        prefix        (str):  The prefix string to prepend to the object name.
        nokeepdir     (bool): Flag to keep or discard the directory structure in
                              the object name.
    """
    file_path = Path(file_path_str)
    # Extract the base filename if nokeepdir is set; otherwise, use the full file_path
    base_name = file_path.name if nokeepdir else file_path_str

    # Add the prefix to the object_name
    return f"{prefix}{base_name}"


##############################################################################
# Command to upload file or directory
##############################################################################
def cmd_upload(s3manager, args):
    """
    Command to upload files or directories to an S3 bucket.

    This function handles the 'upload' command line argument. It uploads
    either a single file or all files within a directory to the specified S3 bucket.

    Arguments:
        s3manager (S3Manager): An instance of the S3Manager class.
        args (argparse.Namespace): Command line arguments.
    """
    # Check if bucket exists
    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    s3manager.disable_pbar = args.nopbar

    # Local filesystem paths of files to upload
    files_to_upload = []

    if args.filename:
        file_path = Path(args.filename)
        if not file_path.is_file():
            msg(
                "red",
                f"Error: File '{args.filename}' does not exist or is not a file.",
                1,
            )
        files_to_upload.append(file_path)

    if args.dir:
        dir_path = Path(args.dir)
        if not dir_path.is_dir():
            msg("red", f"Error: Directory '{dir_path}' not found", 1)
        # Use os.walk for Python <3.12 compatibility. Migrate to Path.walk() later
        for dirpath, _dirnames, files in os.walk(args.dir):
            for filename in files:
                full_path = Path(dirpath).joinpath(filename)
                files_to_upload.append(full_path)

    if not files_to_upload:
        msg("yellow", "No files found to upload.")
        return

    for file in files_to_upload:
        object_name = upload_construct_object_name(
            str(file), args.prefix, args.nokeepdir
        )
        file_path = str(file)
        msg("cyan", f"Uploading file {file_path} with object name {object_name}")
        try:
            s3manager.upload_file(args.bucket, str(file), object_name)
            msg("green", "  - Upload completed successfully")
        except PermissionError:
            msg("red", f"Error: permission denied to read file {file_path}")
        except botocore.exceptions.UnseekableStreamError:
            msg(
                "red",
                f"Error: could not rewind upload stream for '{file_path}'."
                f"Check file permissions.",
            )
        except FileNotFoundError:
            msg("red", f"Error: File '{file_path}' not found")


def download_and_save_object(s3manager, bucket_name, object_name, versionid, dest_conf):
    """
    Helper function to download a single S3 object.

    Arguments:
        s3manager (S3Manager): Instance of the S3Manager class.
        bucket_name (str): Name of the S3 bucket.
        object_name (str): The S3 object key to download.
        versionid (str, optional): The object version ID to download.
        dest_conf (dict): Dictionary containing:
            - 'local_dir' (str): Local directory to save the file.
            - 'overwrite' (bool): Whether to overwrite an existing local file.

    Returns:
        bool: True if download was successful, False otherwise.
    """
    # Unpack destination config
    local_dir = Path(dest_conf["local_dir"])
    overwrite = dest_conf["overwrite"]

    # Stripping leading slashes to ensure relative join
    safe_object_name = object_name.lstrip("/")
    # Set full file path to store the object
    dest_path = local_dir.joinpath(safe_object_name)

    msg("cyan", f"Downloading object {object_name} to path {dest_path}")

    # Check if destination file already exists and overwrite is false
    if not overwrite and dest_path.is_file():
        msg("red", f"Error: File {dest_path} exists. Use --overwrite to replace it.")
        return False

    # Create parent directories if they don't exist
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        msg("red", f"Error: Permission denied to create directory {dest_path.parent}")
        return False

    try:
        s3manager.download_object(bucket_name, object_name, str(dest_path), versionid)
    except PermissionError:
        msg("red", f"Error: Permission denied to write file {dest_path}")
        return False
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "404":
            msg("red", f"Error:  object '{object_name}' not found.")
        else:
            # Re-raise any other Boto3 error
            raise
        return False
    except Exception as e:
        msg("red", f"An unexpected error occurred: {e}")
        return False

    msg("green", "  - Download completed successfully")
    return True


##############################################################################
# Command to download objects
##############################################################################
def cmd_download(s3manager, args):
    """Handle download option."""
    # Check if bucket exists
    if not s3manager.check_bucket_exist(args.bucket):
        msg("red", f"Error: Bucket '{args.bucket}' does not exist", 1)

    s3manager.disable_pbar = args.nopbar

    # Check if local directory exists
    local_path = Path(args.localdir)
    if not local_path.is_dir():
        msg("red", f"Error: local path '{local_path}' is not a valid directory", 1)

    # Create the destination config dictionary
    dest_config = {"local_dir": args.localdir, "overwrite": args.overwrite}

    # Download a specific object
    if args.filename:
        success = download_and_save_object(
            s3manager=s3manager,
            bucket_name=args.bucket,
            object_name=args.filename,
            versionid=args.versionid,
            dest_conf=dest_config,
        )
        if not success:
            sys.exit(1)

    # Download all objects with a prefix
    if args.prefix:
        msg("cyan", f"Downloading all objects with prefix '{args.prefix}'")
        try:
            objects_to_download = list(
                s3manager.list_objects(args.bucket, prefix=args.prefix)
            )
        except botocore.exceptions.ClientError as e:
            msg("red", f"Error listing objects with prefix: {e}", 1)

        if not objects_to_download:
            msg("yellow", f"No objects found with prefix '{args.prefix}'")
            return

        for obj in objects_to_download:
            download_and_save_object(
                s3manager=s3manager,
                bucket_name=args.bucket,
                object_name=obj.key,
                versionid=None,
                dest_conf=dest_config,
            )


##############################################################################
# Main function
##############################################################################
def main():
    """Command line execution."""

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
    log_level = logging.DEBUG if args.debug else logging.WARNING
    setup_logging(log_level)
    logging.debug("CMD line args: %s", args)

    try:
        config = Config(
            profile_name=args.aws_profile,
            region_name=args.region_name,
            s3_endpoint=args.endpoint,
        )
    except ValueError as error:
        msg("red", str(error), 1)

    s3manager = S3Manager(config, checksum_policy=args.checksum_policy)

    # Execute the function (command)
    if args.command is not None:
        args.func(s3manager, args)


##############################################################################
# Run from command line
##############################################################################
if __name__ == "__main__":
    main()

# vim: ts=4
