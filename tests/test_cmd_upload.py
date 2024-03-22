# -*- coding: utf-8 -*-
"""Test cmd_upload function."""

import os
from unittest.mock import MagicMock, Mock, patch

from s3_client import s3_client


def test_cmd_upload_file(s3, tmp_filename):
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
    s3_client.log = Mock()
    with patch("s3_client.s3_client.upload_file_to_s3") as mock_upload:
        s3_client.cmd_upload(s3, args)
        mock_upload.assert_called_once()
        called_args, _ = mock_upload.call_args
        assert called_args[0] == s3
        assert called_args[1] == args.bucket
        assert called_args[2] == args.filename
        assert called_args[3] == args.filename


def test_cmd_upload_directory(s3, tmp_filename):
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
    s3_client.log = Mock()
    with patch("s3_client.s3_client.upload_file_to_s3") as mock_upload:
        s3_client.cmd_upload(s3, args)
        called_args, _ = mock_upload.call_args
        assert mock_upload.call_count == 1
        assert called_args[0] == s3
        assert called_args[1] == args.bucket
        assert called_args[2] == tmp_filename
        assert called_args[3] == tmp_filename


def test_cmd_upload_directory_two_files(s3, directory_with_two_files):
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

    s3_client.log = Mock()
    with patch("s3_client.s3_client.upload_file_to_s3") as mock_upload:
        s3_client.cmd_upload(s3, args)
        assert mock_upload.call_count == 2
        mock_upload.assert_any_call(s3, "test-bucket", str(file1), str(file1))
        mock_upload.assert_any_call(s3, "test-bucket", str(file2), str(file2))
