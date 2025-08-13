# -*- coding: utf-8 -*-
"""Test Download class."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from s3_client import s3_client


def test_download_file_existing_file_overwrite_false(download, tmp_filename):
    """
    Verify that Download.download_file() emits an error message and does not attempt a download
    when the destination file already exists and overwrite=False.
    """
    download.local_dir = Path(tmp_filename).parent

    s3_client.msg = Mock()
    download.s3 = Mock()
    download.s3.download_object = Mock()

    # pass only file name, dir was set in local_dir
    download.download_file(object_name=Path(tmp_filename).name, overwrite=False)

    expected_dest = str(tmp_filename)

    s3_client.msg.assert_any_call(
        "red",
        f"Error: File {expected_dest} exists. Use --overwrite to replace it.",
        1,
    )


def test_download_file_existing_file_overwrite_true(download, tmp_filename):
    """
    Verify that Download.download_file() calls S3 download_object and emits a success message
    when the destination file already exists and overwrite=True.
    """
    s3_client.msg = Mock()
    download.s3 = Mock()
    download.s3.download_object = Mock()

    # Set to temp dir
    download.local_dir = Path(tmp_filename).parent

    # Use only the basename, dir was set in local_dir
    object_name = Path(tmp_filename).name

    # pass only file name, dir was set in local_dir
    download.download_file(object_name=object_name, overwrite=True)

    # Ensure download_object was called once with the right args
    download.s3.download_object.assert_called_once_with(
        download.bucket_name, object_name, str(tmp_filename), None
    )

    # Ensure the success message was printed
    s3_client.msg.assert_any_call("green", "  - Download completed successfully")


def test_download_permission_error(download, tmp_filename):
    """
    Verify that download_file handles PermissionError raised by the S3 client.
    """
    s3_client.msg = Mock()
    download.s3 = Mock()
    download.s3.download_object = Mock(side_effect=PermissionError)

    # Set to temp dir
    download.local_dir = Path(tmp_filename).parent

    # Use only the basename, dir was set in local_dir
    object_name = Path(tmp_filename).name

    download.download_file(object_name=object_name, overwrite=True)

    s3_client.msg.assert_any_call(
        "red", f"Error: Permission denied to write file {tmp_filename}", 1
    )


@pytest.mark.parametrize(
    "localdir, objectname, dest_path",
    [
        (".", "a", "a"),
        (".", "/a", "a"),
        ("./", "a", "a"),
        ("./", "/a", "a"),
        ("dir", "a", "dir/a"),
        ("dir/", "a", "dir/a"),
        ("dir", "/a", "dir/a"),
        ("dir/", "/a", "dir/a"),
        ("/", "a", "/a"),
        ("/", "/a", "/a"),
        ("/.", "/a", "/a"),
        ("./x/y", "a", "x/y/a"),
        ("/x/y", "a", "/x/y/a"),
        ("/x/y/", "a", "/x/y/a"),
        ("/x/y", "/a", "/x/y/a"),
        ("/x/y/", "/a", "/x/y/a"),
        ("/x/y/.", "/a", "/x/y/a"),
    ],
)
def test_download_file_dest_path_variations(
    download, tmp_path, localdir, objectname, dest_path
):
    """
    Verify that Download.download_file() constructs dest_path correctly
    for various combinations of local_dir and object_name.
    """
    s3_client.msg = Mock()
    download.s3 = Mock()
    download.s3.download_object = Mock()

    # Set to temp dir
    download.local_dir = Path(localdir)

    with patch.object(Path, "mkdir"):
        # pass objectname
        download.download_file(object_name=objectname, overwrite=True)

    # Extract the dest_path actually passed to download_object
    _, _, dest_used, _ = download.s3.download_object.call_args[0]

    assert dest_used == dest_path
