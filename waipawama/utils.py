from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic.main import ModelMetaclass


class DataTemplate:
    """Serialize a list of pydantic models to get a dataframe and more."""
    def __init__(self, template: ModelMetaclass, by_alias: bool = False):
        self.template: ModelMetaclass = template
        self.by_alias: bool = by_alias

    def __repr__(self):
        return f"DataTemplate({self.template().__dict__})"

    def __str__(self):
        return str(self.template())

    @property
    def default(self):
        """Return a single dict containing the default values"""
        return self.template().dict(by_alias=self.by_alias)

    def record(self, record: Optional[Dict] = None):
        """Generate a single dict from the template"""
        if record is None:
            record = {}
        return self.template(**record).dict(by_alias=self.by_alias)

    def records(self, records: List[Dict]) -> List[Dict]:
        """Generate a list of dicts conforming to the template"""
        return [self.template(**record).dict(by_alias=self.by_alias) for record in records]

    def dataframe(self, records: List[Dict]) -> Any:
        """Generate a pandas dataframe from a list of dicts"""
        return pd.DataFrame(self.records(records))
