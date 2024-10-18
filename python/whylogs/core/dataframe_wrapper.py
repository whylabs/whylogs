from typing import List, Optional, Union

from whylogs.core.stubs import pd, pl


class DataFrameWrapper:
    def __init__(self, pandas: Optional[pd.DataFrame]=None, polars: Optional[pl.DataFrame]=None):
        if pandas is not None and polars is not None:
            raise ValueError("Cannot pass both pandas and polars params")
        if pandas is None and polars is None:
            raise ValueError("Must pass either pandas or polars")

        self.pd_df = pandas
        self.pl_df = polars

        self.column_names = list(pandas.columns) if pandas is not None else polars.columns
        self.dtypes = pandas.dtypes if pandas is not None else polars.schema
        self.empty = pandas.empty if pandas is not None else len(polars) == 0

    def get(self, column: str) -> Optional[Union[pd.Series, pl.Series]]:
        if self.pd_df is not None:
            return self.pd_df.get(column)
        return self.pl_df[column] if column in self.pl_df.schema else None
