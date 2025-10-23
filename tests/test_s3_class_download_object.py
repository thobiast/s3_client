# -*- coding: utf-8 -*-
"""Test s3 class."""

from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from conftest import BUCKET_NAME

from s3_client import s3_client


def test_download_object(s3, s3_bucket, tmpdir):
    """Test object download."""
    s3_client.log = Mock()
    obj_name = "my_object"
    obj_body = "Test object content"
    dest_name = "{}/{}".format(tmpdir, obj_name)
    s3_bucket.Bucket(BUCKET_NAME).put_object(Key=obj_name, Body=obj_body)
    s3.download_object(BUCKET_NAME, obj_name, dest_name)
    assert (tmpdir / obj_name).read() == obj_body


@pytest.mark.parametrize(
    "extraargs, versionid", [(None, None), ({"VersionId": "1234567890"}, "1234567890")]
)
def test_download_object_extraargs(s3, extraargs, versionid):
    """Test download object ExtraArgs option."""
    s3_client.log = Mock()
    obj_name = "my_object"
    dest_name = "/tmp"

    # Patch the S3 resource used by the S3 class
    with patch.object(s3, "s3_resource") as mock_res:
        # Ensure numeric sizes so Progress.add_task(total=...) works
        mock_res.ObjectSummary.return_value.size = 1
        mock_res.ObjectVersion.return_value.head.return_value = {"ContentLength": 1}

        # Patch rich.progress.Progress as a context manager
        with patch.object(s3_client, "progress") as mock_progress_module:
            pbar_cm = MagicMock()
            pbar_cm.__enter__.return_value = pbar_cm
            pbar_cm.add_task.return_value = 1
            mock_progress_module.Progress.return_value = pbar_cm

            s3.download_object(BUCKET_NAME, obj_name, dest_name, versionid)

            s3.s3_resource.Bucket(BUCKET_NAME).download_file.assert_called_with(
                obj_name, dest_name, ExtraArgs=extraargs, Callback=ANY
            )
