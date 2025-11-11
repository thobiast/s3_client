# -*- coding: utf-8 -*-
"""Test s3 class."""

from conftest import BUCKET_NAME, BUCKET_NAME_NOT_EXIST


def test_check_bucket_exist(s3manager, s3_bucket):
    assert s3manager.check_bucket_exist(BUCKET_NAME) is True


def test_check_bucket_does_not_exist(s3manager, s3_bucket):
    assert s3manager.check_bucket_exist(BUCKET_NAME_NOT_EXIST) is False
