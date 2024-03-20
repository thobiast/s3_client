# -*- coding: utf-8 -*-
"""Test config class."""
# flake8: noqa: F841

import pytest

from s3_client import s3_client


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set environment variables"""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "my_aws_key_id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "my_aws_access_key")


def test_config_env_var(mock_env_vars):
    """Check config env variable"""
    config = s3_client.Config()
    assert config.aws_access_key_id == "my_aws_key_id"
    assert config.aws_secret_access_key == "my_aws_access_key"


@pytest.mark.parametrize("envvar", [("AWS_ACCESS_KEY_ID"), ("AWS_SECRET_ACCESS_KEY")])
def test_config_missing_env_var(mock_env_vars, monkeypatch, envvar):
    """Check config missing env var"""
    # Delete env
    monkeypatch.delenv(envvar, raising=False)
    with pytest.raises(
        ValueError,
        match="Error: You must export environment variable {}".format(envvar),
    ):
        config = s3_client.Config()
