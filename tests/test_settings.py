import pathlib

import pytest

from waipawama import settings


def test_file_settings(env):
    env.set('waipawama_data_folder', str(pathlib.Path().home()))
    s = settings.Settings()
    assert s.data_folder == pathlib.Path().home()