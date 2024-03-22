# -*- coding: utf-8 -*-
"""Test s3 class."""

from unittest.mock import Mock

import pytest
from conftest import BUCKET_NAME, KEY_NAMES

from s3_client import s3_client


@pytest.mark.parametrize("prefix", [("t"), ("test")])
def test_list_objects_prefix(s3, s3_objects, prefix):
    s3_client.log = Mock()
    num_keys = [i for i in KEY_NAMES if i.startswith(prefix)]
    result = s3.list_objects(BUCKET_NAME, prefix=prefix)
    assert len(num_keys) == len([x.key for x in result])


def test_list_objects(s3, s3_objects):
    s3_client.log = Mock()
    result = s3.list_objects(BUCKET_NAME)
    assert len(KEY_NAMES) == len([x.key for x in result])
    assert KEY_NAMES[0] == [x.key for x in result][0]


@pytest.mark.parametrize("limit", [(1), (2), (3), (4)])
def test_list_objects_limit(s3, s3_objects, limit):
    s3_client.log = Mock()
    result = s3.list_objects(BUCKET_NAME, limit=limit)
    assert limit == len([x.key for x in result])
    assert KEY_NAMES[0] == [x.key for x in result][0]
