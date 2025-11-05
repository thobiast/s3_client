# -*- coding: utf-8 -*-
"""Test s3 class."""
# flake8: noqa: F841

from unittest.mock import Mock, patch

from conftest import BUCKET_NAME

from s3_client import main

DELETE_KEY = "A"


def test_delete_object(s3manager, s3_objects):
    """Test delete object."""
    result = s3manager.delete_object(BUCKET_NAME, DELETE_KEY)

    for obj in s3manager.s3_resource.Bucket(BUCKET_NAME).objects.all():
        assert obj.key != DELETE_KEY


def test_delete_object_no_version(s3manager, s3_objects):
    """Test delete object without version id option."""
    obj_to_delete = {"Key": DELETE_KEY}

    with patch.object(s3manager, "s3_resource") as mock_s3_resource:
        mock_s3_resource.meta.client = Mock()

        result = s3manager.delete_object(BUCKET_NAME, DELETE_KEY)

        mock_s3_resource.meta.client.delete_object.assert_called_with(
            Bucket=BUCKET_NAME,
            Key=DELETE_KEY,
        )


def test_delete_object_version(s3manager, s3_objects):
    """Test delete object with specific version id."""
    version_id = "1234567890"

    with patch.object(s3manager, "s3_resource") as mock_s3_resource:
        mock_s3_resource.meta.client = Mock()

        result = s3manager.delete_object(BUCKET_NAME, DELETE_KEY, version_id)
        mock_s3_resource.meta.client.delete_object.assert_called_with(
            Bucket=BUCKET_NAME,
            Key=DELETE_KEY,
            VersionId=version_id,
        )
