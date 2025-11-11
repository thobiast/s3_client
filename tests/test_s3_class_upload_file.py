# -*- coding: utf-8 -*-
"""Test s3 class."""

from conftest import BUCKET_NAME, TMP_FILENAME


def test_upload_file(tmp_filename, s3manager, s3_bucket):
    s3manager.upload_file(BUCKET_NAME, tmp_filename)
    body = (
        s3_bucket.Object(BUCKET_NAME, tmp_filename).get()["Body"].read().decode("utf-8")
    )
    assert body == TMP_FILENAME


def test_upload_file_with_keyname(tmp_filename, s3manager, s3_bucket):
    key_name = "other_name"
    s3manager.upload_file(BUCKET_NAME, tmp_filename, key_name)
    body = s3_bucket.Object(BUCKET_NAME, key_name).get()["Body"].read().decode("utf-8")
    assert body == TMP_FILENAME
