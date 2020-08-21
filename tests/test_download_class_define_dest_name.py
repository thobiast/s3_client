# -*- coding: utf-8 -*-
"""Test Download class."""

import pytest


@pytest.mark.parametrize(
    "localdir, objectname, result",
    [
        (".", "a", "./a"),
        (".", "/a", "./a"),
        ("./", "a", "./a"),
        ("./", "/a", "./a"),
        ("dir", "a", "dir/a"),
        ("dir/", "a", "dir/a"),
        ("dir", "/a", "dir/a"),
        ("dir/", "/a", "dir/a"),
        ("/", "a", "/a"),
        ("/", "/a", "/a"),
        ("/.", "/a", "/./a"),
        ("/x/y", "a", "/x/y/a"),
        ("/x/y/", "a", "/x/y/a"),
        ("/x/y", "/a", "/x/y/a"),
        ("/x/y/", "/a", "/x/y/a"),
        ("/x/y/.", "/a", "/x/y/./a"),
    ],
)
def test_build_dest_name(download, localdir, objectname, result):
    download.local_dir = localdir
    assert download.define_dest_name(objectname) == result
