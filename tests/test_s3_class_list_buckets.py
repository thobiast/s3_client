# -*- coding: utf-8 -*-
"""Test s3 class."""

import pytest
from unittest.mock import Mock
import moto

from s3_client import s3_client

from conftest import BUCKET_NAME


def test_list_buckets(s3, s3_bucket):
    s3_client.log = Mock()
    result = s3.list_buckets()
    assert BUCKET_NAME == [x.name for x in result][0]
    assert 1 == len([x.name for x in result])


@moto.mock_s3()
def test_list_buckets_empty(s3):
    s3_client.log = Mock()
    result = s3.list_buckets()
    assert 0 == len([x.name for x in result])
