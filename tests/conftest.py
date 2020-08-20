# -*- coding: utf-8 -*-

import boto3
import pytest
import moto

from s3_client import s3_client

BUCKET_NAME = "my_bucket"
BUCKET_NAME_NOT_EXIST = "my_bucket_does_not_exist"
KEY_NAMES = ["A", "B", "t01", "t02", "test01", "test02", "test03"]

TMP_FILENAME = "my_file"


@pytest.fixture(scope="function")
def s3():
    return s3_client.S3("aws_key_id", "aws_access_key", "https://s3.amazonaws.com")


@pytest.fixture(scope="function")
def s3_bucket():
    with moto.mock_s3():
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture(scope="function")
def s3_objects():
    with moto.mock_s3():
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket=BUCKET_NAME)
        for key_name in KEY_NAMES:
            conn.meta.client.put_object(Bucket=BUCKET_NAME, Key=key_name, Body="body")
        yield conn


@pytest.fixture(scope="function")
def download():
    return s3_client.Download("s3", "my_bucket", ".")


@pytest.fixture(scope="function")
def tmp_filename(tmpdir):
    """Creates a temporary file and return full pathname."""
    p = tmpdir.join(TMP_FILENAME)
    p.write(TMP_FILENAME)
    return str(p)
