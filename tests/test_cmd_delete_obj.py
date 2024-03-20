# -*- coding: utf-8 -*-
"""Test cmd delete obj."""

from unittest.mock import Mock, patch

from s3_client import s3_client


def test_cmd_delete_obj(s3):
    """Test cmd_delete_obj function."""
    args = Mock()
    args.bucket = "my_bucket"
    args.object = "my_object"
    args.versionid = "1234567890"
    with patch.object(s3, "check_bucket_exist", return_value=True):
        with patch.object(s3, "delete_object"):
            s3_client.cmd_delete_obj(s3, args)
            s3.delete_object.assert_called_with(
                args.bucket, args.object, args.versionid
            )


def test_cmd_delete_obj_bucket_not_exist(s3):
    """Test if bucket does not exist."""
    s3_client.msg = Mock()
    args = Mock()
    args.bucket = "my_bucket"
    args.object = "my_object"
    with patch.object(s3, "check_bucket_exist", return_value=False):
        with patch.object(s3, "delete_object"):
            s3_client.cmd_delete_obj(s3, args)
            s3_client.msg.assert_called_with(
                "red", "Error: Bucket 'my_bucket' does not exist", 1
            )
