# -*- coding: utf-8 -*-
"""Test Download class."""

from unittest.mock import Mock

from conftest import TMP_FILENAME

from s3_client import s3_client


def test_download_file(download, tmp_filename):
    obj_name = TMP_FILENAME
    dest_name = str(tmp_filename)
    s3_client.msg = Mock()
    s3_client.create_dir = Mock()
    download.s3 = Mock()
    download.s3.download_object = Mock()
    download.check_file_exist = Mock()
    download.define_dest_name = Mock()
    download.define_dest_name.return_value = dest_name
    download.download_file(obj_name, True)
    download.s3.download_object.assert_called_once_with(
        download.bucket_name, obj_name, dest_name, None
    )


def test_download_file_permissionerror(download):
    s3_client.msg = Mock()
    s3_client.create_dir = Mock()
    dest_name = "/tmp/myfile"
    download.define_dest_name = Mock()
    download.define_dest_name.return_value = dest_name
    download.s3 = Mock()
    download.s3.download_object = Mock(side_effect=PermissionError)
    download.download_file(TMP_FILENAME, True)
    s3_client.msg.assert_any_call(
        "red", "Error: Permission denied to write file {}".format(dest_name), 1
    )


def test_download_file_not_overwrite(download, tmp_filename):
    obj_name = TMP_FILENAME
    dest_name = str(tmp_filename)
    s3_client.msg = Mock()
    s3_client.create_dir = Mock()
    download.s3 = Mock()
    download.check_file_exist = Mock()
    download.define_dest_name = Mock()
    download.define_dest_name.return_value = dest_name
    download.download_file(obj_name, False)
    download.check_file_exist.assert_called_once_with(dest_name)


def test_download_file_overwrite(download, tmp_filename):
    obj_name = TMP_FILENAME
    dest_name = str(tmp_filename)
    s3_client.msg = Mock()
    s3_client.create_dir = Mock()
    download.s3 = Mock()
    download.check_file_exist = Mock()
    download.define_dest_name = Mock()
    download.define_dest_name.return_value = dest_name
    download.download_file(obj_name, True)
    download.check_file_exist.assert_not_called()
