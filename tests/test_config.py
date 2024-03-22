# -*- coding: utf-8 -*-
"""Test config class."""
# flake8: noqa: F841

from unittest.mock import patch

import pytest

from s3_client import s3_client


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set environment variables"""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "my_aws_key_id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "my_aws_access_key")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "my_aws_token")


def test_config_env_var(mock_env_vars):
    """Check config env variable"""
    config = s3_client.Config()
    session = config.get_session()
    assert session.get_credentials().access_key == "my_aws_key_id"
    assert session.get_credentials().secret_key == "my_aws_access_key"
    assert session.get_credentials().token == "my_aws_token"


@pytest.mark.parametrize("envvar", [("AWS_ACCESS_KEY_ID"), ("AWS_SECRET_ACCESS_KEY")])
def test_config_missing_env_var(mock_env_vars, monkeypatch, envvar):
    """Check config missing env var"""
    # Delete env
    monkeypatch.delenv(envvar, raising=False)
    with pytest.raises(
        ValueError,
        match="AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in environment variables.",
    ):
        config = s3_client.Config()


def test_config_initialization(mock_env_vars):
    test_region = "my_region"
    test_endpoint = "https://s3mycompany.com"
    config = s3_client.Config(region_name=test_region, s3_endpoint=test_endpoint)
    assert config.region_name == test_region
    assert config.s3_endpoint == test_endpoint


def test_config_profile_and_region(mock_env_vars):
    test_profile = "test-profile"
    test_region = "my_region"
    with patch("boto3.Session") as MockSession:
        s3_client.Config(profile_name=test_profile, region_name=test_region)

        MockSession.assert_called_once_with(
            profile_name=test_profile, region_name=test_region
        )
