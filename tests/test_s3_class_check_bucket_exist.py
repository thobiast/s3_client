# -*- coding: utf-8 -*-
"""Test s3 class."""

import pytest
from unittest.mock import Mock
import boto3
import moto

from s3_client import s3_client

from conftest import BUCKET_NAME, BUCKET_NAME_NOT_EXIST


def test_check_bucket_exist(s3, s3_bucket):
    s3_client.log = Mock()
    assert s3.check_bucket_exist(BUCKET_NAME) == True


def test_check_bucket_does_not_exist(s3, s3_bucket):
    s3_client.log = Mock()
    assert s3.check_bucket_exist(BUCKET_NAME_NOT_EXIST) == False


@moto.mock_s3
def test_check_bucket_already_checked(s3):
    s3_client.log = Mock()
    s3.buckets_exist = ["my_bucket"]
    assert s3.check_bucket_exist("my_bucket") == True
