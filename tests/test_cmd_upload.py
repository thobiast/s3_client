# -*- coding: utf-8 -*-
"""Test cmd_upload function."""

import os
from unittest.mock import MagicMock, patch

import pytest

from s3_client import main


def test_cmd_upload_file(s3manager, tmp_filename):
    """
    Test cmd_upload function to ensure it handles file uploads correctly.
    """
    args = MagicMock(
        bucket="test-bucket",
        filename=tmp_filename,
        dir=None,
        nopbar=True,
        nokeepdir=False,
        prefix="",
    )
    with patch.object(s3manager, "check_bucket_exist", return_value=True), patch.object(
        s3manager, "upload_file"
    ) as mock_upload:
        main.cmd_upload(s3manager, args)
        mock_upload.assert_called_once()
        called_args, _ = mock_upload.call_args
        assert called_args[0] == args.bucket
        assert called_args[1] == args.filename
        assert called_args[2] == args.filename


def test_cmd_upload_directory(s3manager, tmp_filename):
    """
    Test cmd_upload function to ensure it handles directory uploads correctly.
    """
    args = MagicMock(
        bucket="test-bucket",
        filename=None,
        dir=str(os.path.dirname(tmp_filename)),
        nopbar=True,
        nokeepdir=False,
        prefix="",
    )
    with patch.object(s3manager, "check_bucket_exist", return_value=True), patch.object(
        s3manager, "upload_file"
    ) as mock_upload:
        main.cmd_upload(s3manager, args)
        called_args, _ = mock_upload.call_args
        assert mock_upload.call_count == 1
        assert called_args[0] == args.bucket
        assert called_args[1] == tmp_filename
        assert called_args[2] == tmp_filename


def test_cmd_upload_directory_two_files(s3manager, directory_with_two_files):
    """
    Test cmd_upload function with two files under directory.
    """
    tmp_path, file1, file2 = directory_with_two_files

    args = MagicMock(
        bucket="test-bucket",
        dir=str(tmp_path),
        filename=None,
        nopbar=True,
        nokeepdir=False,
        prefix="",
    )

    with patch.object(s3manager, "check_bucket_exist", return_value=True), patch.object(
        s3manager, "upload_file"
    ) as mock_upload:
        main.cmd_upload(s3manager, args)
        assert mock_upload.call_count == 2
        mock_upload.assert_any_call("test-bucket", str(file1), str(file1))
        mock_upload.assert_any_call("test-bucket", str(file2), str(file2))


def test_cmd_upload_bucket_not_exist(s3manager, tmp_filename):
    """
    Test cmd_upload function if bucket does not exist.
    """
    args = MagicMock(
        bucket="missing",
        filename=tmp_filename,
        dir=None,
        nopbar=True,
        nokeepdir=False,
        prefix="",
    )
    with patch.object(
        s3manager, "check_bucket_exist", return_value=False
    ), patch.object(s3manager, "upload_file") as mock_upload_file, patch.object(
        main, "msg", side_effect=SystemExit
    ) as mock_msg:

        with pytest.raises(SystemExit):
            main.cmd_upload(s3manager, args)
        mock_msg.assert_called_with("red", "Error: Bucket 'missing' does not exist", 1)
        mock_upload_file.assert_not_called()
