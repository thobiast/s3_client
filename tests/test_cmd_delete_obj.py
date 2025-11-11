# -*- coding: utf-8 -*-
"""Test cmd delete obj."""

from unittest.mock import Mock, patch

import pytest

from s3_client import main


@pytest.mark.parametrize("versionid", ["1234567890", None])
@patch("s3_client.main.pprint.pprint")
def test_cmd_delete_obj(mock_pprint, s3manager, versionid):
    """Test cmd_delete_obj function."""
    args = Mock()
    args.bucket = "my_bucket"
    args.object = "my_object"
    args.versionid = versionid

    with patch.object(s3manager, "check_bucket_exist", return_value=True):
        with patch.object(
            s3manager, "delete_object", return_value={"ok": True}
        ) as mock_delete_object:
            main.cmd_delete_obj(s3manager, args)
            mock_delete_object.assert_called_with(args.bucket, args.object, versionid)
            mock_pprint.assert_called_with({"ok": True})


def test_cmd_delete_obj_bucket_not_exist(s3manager):
    """Test if bucket does not exist."""
    with patch.object(main, "msg", side_effect=SystemExit) as mock_msg:
        args = Mock()
        args.bucket = "my_bucket"
        args.object = "my_object"
        args.versionid = None

        with patch.object(s3manager, "check_bucket_exist", return_value=False):
            with patch.object(s3manager, "delete_object") as mock_delete_object:
                with pytest.raises(SystemExit):
                    main.cmd_delete_obj(s3manager, args)
                mock_msg.assert_called_with(
                    "red", "Error: Bucket 'my_bucket' does not exist", 1
                )
                mock_delete_object.assert_not_called()
