# -*- coding: utf-8 -*-
"""Test s3 class."""

import moto
from conftest import BUCKET_NAME


def test_list_buckets(s3manager, s3_bucket):
    result = s3manager.list_buckets()
    assert BUCKET_NAME == [x.name for x in result][0]
    assert 1 == len([x.name for x in result])


@moto.mock_aws()
def test_list_buckets_empty(s3manager):
    result = s3manager.list_buckets()
    assert 0 == len([x.name for x in result])
