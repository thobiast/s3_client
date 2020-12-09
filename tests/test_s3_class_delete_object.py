# -*- coding: utf-8 -*-
"""Test s3 class."""

import pytest
from unittest.mock import Mock

from s3_client import s3_client

from conftest import KEY_NAMES, BUCKET_NAME

DELETE_KEY = "A"


def test_delete_object(s3, s3_objects):
    s3_client.log = Mock()
    result = s3.delete_object(BUCKET_NAME, DELETE_KEY)
    for obj in s3.s3_resource.Bucket(BUCKET_NAME).objects.all():
        assert obj.key != DELETE_KEY
