# -*- coding: utf-8 -*-
"""Test s3 class."""

from unittest.mock import Mock

from conftest import BUCKET_NAME, BUCKET_NAME_NOT_EXIST

from s3_client import s3_client


def test_check_bucket_exist(s3, s3_bucket):
    s3_client.log = Mock()
    assert s3.check_bucket_exist(BUCKET_NAME) is True


def test_check_bucket_does_not_exist(s3, s3_bucket):
    s3_client.log = Mock()
    assert s3.check_bucket_exist(BUCKET_NAME_NOT_EXIST) is False
