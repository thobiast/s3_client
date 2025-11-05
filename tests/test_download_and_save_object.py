# -*- coding: utf-8 -*-
"""Test download_and_save_object."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from s3_client import main


def test_download_file_existing_file_overwrite_false(tmp_filename):
    """
    When destination exists and overwrite=False, helper must emit an error,
    not call s3manager.download_object, and return False.
    """
    local_dir = Path(tmp_filename).parent
    object_name = Path(tmp_filename).name
    expected_dest = str(tmp_filename)

    main.msg = Mock()
    main.s3manager = Mock()
    main.s3manager.download_object = Mock()

    success = main.download_and_save_object(
        s3manager=main.s3manager,
        bucket_name="bkt",
        object_name=object_name,
        versionid=None,
        dest_conf={"local_dir": local_dir, "overwrite": False},
    )

    assert success is False
    main.s3manager.download_object.assert_not_called()
    main.msg.assert_any_call(
        "red",
        f"Error: File {expected_dest} exists. Use --overwrite to replace it.",
    )


def test_download_file_existing_file_overwrite_true(tmp_filename):
    """
    When destination exists and overwrite=True, it must call s3manager.download_object,
    show success message, and return True.
    """
    main.msg = Mock()
    main.s3manager = Mock()
    main.s3manager.download_object = Mock()

    # Set to temp dir
    local_dir = Path(tmp_filename).parent
    # Use only the basename, dir was set in local_dir
    object_name = Path(tmp_filename).name

    success = main.download_and_save_object(
        s3manager=main.s3manager,
        bucket_name="bkt",
        object_name=object_name,
        versionid=None,
        dest_conf={"local_dir": local_dir, "overwrite": True},
    )

    assert success is True
    main.s3manager.download_object.assert_called_once_with(
        "bkt", object_name, str(tmp_filename), None
    )
    # Ensure the success message was printed
    main.msg.assert_any_call("green", "  - Download completed successfully")


def test_download_permission_error(tmp_filename):
    """
    PermissionError raised by s3 client should be caught and return False,
    with error message.
    """
    main.msg = Mock()
    main.s3manager = Mock()
    main.s3manager.download_object = Mock(side_effect=PermissionError)

    # Set to temp dir
    local_dir = Path(tmp_filename).parent

    # Use only the basename, dir was set in local_dir
    object_name = Path(tmp_filename).name

    success = main.download_and_save_object(
        s3manager=main.s3manager,
        bucket_name="bkt",
        object_name=object_name,
        versionid=None,
        dest_conf={"local_dir": local_dir, "overwrite": True},
    )

    assert success is False
    main.msg.assert_any_call(
        "red", f"Error: Permission denied to write file {tmp_filename}"
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
def test_download_file_dest_path_variations(tmp_path, localdir, objectname, dest_path):
    """
    Verify that function constructs dest_path correctly
    for various combinations of local_dir and object_name.
    """
    main.msg = Mock()
    main.s3manager = Mock()
    main.s3manager.download_object = Mock()

    # Set to temp dir
    local_dir = Path(localdir)

    with patch.object(Path, "mkdir"):
        success = main.download_and_save_object(
            s3manager=main.s3manager,
            bucket_name="bkt",
            object_name=objectname,
            versionid=None,
            dest_conf={"local_dir": local_dir, "overwrite": True},
        )

    # Extract the dest_path actually passed to download_object
    _, _, dest_used, _ = main.s3manager.download_object.call_args[0]
    assert dest_used == dest_path
    assert success is True
