# -*- coding: utf-8 -*-
"""Test Download class."""

from unittest.mock import Mock

from s3_client import s3_client


def test_check_file_exist(download, tmp_filename):
    s3_client.msg = Mock()
    file_name = str(tmp_filename)
    download.check_file_exist(file_name)
    s3_client.msg.assert_called_once_with(
        "red",
        "Error: File {} exist. Remove it from local drive to download.".format(
            file_name
        ),
        1,
    )


def test_check_file_exist_not_exist(download, tmp_filename):
    s3_client.msg = Mock()
    file_name = str(tmp_filename) + "any"
    download.check_file_exist(file_name)
    s3_client.msg.assert_not_called()
