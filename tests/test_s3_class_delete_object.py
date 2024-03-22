# -*- coding: utf-8 -*-
"""Test s3 class."""
# flake8: noqa: F841

from unittest.mock import Mock, patch

from conftest import BUCKET_NAME

from s3_client import s3_client

DELETE_KEY = "A"


def test_delete_object(s3, s3_objects):
    """Test delete object."""
    s3_client.log = Mock()
    result = s3.delete_object(BUCKET_NAME, DELETE_KEY)
    for obj in s3.s3_resource.Bucket(BUCKET_NAME).objects.all():
        assert obj.key != DELETE_KEY


def test_delete_object_no_version(s3, s3_objects):
    """Test delete object without version id option."""
    s3_client.log = Mock()

    obj_to_delete = {"Key": DELETE_KEY}

    with patch.object(s3, "s3_resource"):
        result = s3.delete_object(BUCKET_NAME, DELETE_KEY)
        s3.s3_resource.Bucket(BUCKET_NAME).delete_objects.assert_called_with(
            Delete={"Objects": [obj_to_delete]}
        )


def test_delete_object_version(s3, s3_objects):
    """Test delete object with specific version id."""
    s3_client.log = Mock()

    version_id = "1234567890"
    obj_to_delete = {"Key": DELETE_KEY, "VersionId": version_id}

    with patch.object(s3, "s3_resource"):
        result = s3.delete_object(BUCKET_NAME, DELETE_KEY, version_id)
        s3.s3_resource.Bucket(BUCKET_NAME).delete_objects.assert_called_with(
            Delete={"Objects": [obj_to_delete]}
        )
