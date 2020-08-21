# -*- coding: utf-8 -*-
"""Test s3 class."""

import pytest
from unittest.mock import Mock

from s3_client import s3_client

from conftest import BUCKET_NAME


def test_download_object(s3, s3_bucket, tmpdir):
    s3_client.log = Mock()
    obj_name = "my_object"
    obj_body = "Test object content"
    dest_name = "{}/{}".format(tmpdir, obj_name)
    s3_bucket.Bucket(BUCKET_NAME).put_object(Key=obj_name, Body=obj_body)
    s3.download_object(BUCKET_NAME, obj_name, dest_name)
    assert (tmpdir / obj_name).read() == obj_body
