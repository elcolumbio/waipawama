"""
Settings management with pydantics.
See pydantic docs: https://pydantic-docs.helpmanual.io/usage/settings/
With pydantics we get type checking and error messages.
Also you can choose between parmater passing and env variables.
"""
import pandas
import pathlib
from pydantic import (
    BaseSettings,
    DirectoryPath,
    RedisDsn,
    PostgresDsn,
)


class Settings(BaseSettings):
    """
    Option 1: Settings(data_folder = pathlib.Path()).
    Option 2: Set your environment variables according to Config.
    """
    data_folder: DirectoryPath = None # checks if path exists, alt pathlib.Path
    tmp_folder: DirectoryPath = None

    redis_dsn: RedisDsn = 'redis://user:pass@localhost:6379/1'
    pg_dsn: PostgresDsn = 'postgres://user:pass@localhost:5432/foobar'
        
    class Config:
        env_prefix = 'waipawama_'