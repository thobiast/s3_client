# -*- coding: utf-8 -*-
"""Test function upload_construct_object_name."""

import pytest

from s3_client import s3_client


@pytest.mark.parametrize(
    "file_path, prefix, nokeepdir, expected",
    [
        ("path/to/file.txt", "", False, "path/to/file.txt"),
        ("path/to/file.txt", "", True, "file.txt"),
        ("path/to/file.txt", "prefix-", False, "prefix-path/to/file.txt"),
        ("path/to/file.txt", "prefix-", True, "prefix-file.txt"),
        ("file.txt", "prefix-", False, "prefix-file.txt"),
        ("file.txt", "prefix-", True, "prefix-file.txt"),
    ],
)
def test_upload_construct_object_name(file_path, prefix, nokeepdir, expected):
    object_name = s3_client.upload_construct_object_name(file_path, prefix, nokeepdir)
    assert object_name == expected
