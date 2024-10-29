from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from whylogs.core.stubs import pd, pl


class DataFrameWrapper:
    def __init__(self, pandas: Optional[pd.DataFrame] = None, polars: Optional[pl.DataFrame] = None):
        # TODO: __init__(self, df: Union[pd.DataFrame, pl.DataFrame]): with isinstance
        # TODO: maybe PandasDataFrame, PolarsDataFrame <: DataFrameWrapper
        if pandas is not None and polars is not None:
            raise ValueError("Cannot pass both pandas and polars params")
        if pandas is None and polars is None:
            raise ValueError("Must pass either pandas or polars")

        self.pd_df = pandas
        self.pl_df = polars

        self.column_names = list(pandas.columns) if pandas is not None else polars.columns  # type: ignore
        self.dtypes = pandas.dtypes if pandas is not None else polars.schema  # type: ignore
        self.empty = pandas.empty if pandas is not None else len(polars) == 0  # type: ignore

    def _update(self) -> None:
        self.column_names = list(self.pd_df.columns) if self.pd_df is not None else self.pl_df.columns  # type: ignore
        self.dtypes = self.pd_df.dtypes if self.pd_df is not None else self.pl_df.schema  # type: ignore
        self.empty = self.pd_df.empty if self.pd_df is not None else len(self.pl_df) == 0  # type: ignore

    def get(self, column: str) -> Optional[Union[pd.Series, pl.Series]]:
        if self.pd_df is not None:
            return self.pd_df.get(column)
        return self.pl_df[column] if column in self.pl_df.schema else None  # type: ignore

    def filter(self, filter: Any) -> Optional["DataFrameWrapper"]:
        if self.pd_df is not None:
            return DataFrameWrapper(pandas=self.pd_df[filter])
        if self.pl_df is not None:
            return DataFrameWrapper(polars=self.pl_df.filter(filter))
        return None

    def query(self, query: str) -> Optional["DataFrameWrapper"]:
        if self.pd_df is not None:
            return DataFrameWrapper(pandas=self.pd_df.query(query))
        if self.pl_df is not None:
            ctx = pl.SQLContext(population=self.pl_df, eager=True)
            return ctx.execute(query)
        return None

    def group_keys(self, columns: List[str]) -> List[Tuple[Any]]:
        if self.pd_df is not None:
            return self.pd_df.groupby(columns).groups.keys()
        elif self.pl_df is not None:
            return [x for x, y in self.pl_df.group_by(columns)]
        return []

    def groupby(
        self, columns: List[str]
    ) -> Any:  # Union[pl.dataframe.group_by.GroupBy, pd.core.groupby.generic.DataFrameGroupBy]
        if self.pd_df is not None:
            grouped = self.pd_df.groupby(columns)
            return grouped
            d = {g: grouped.get_group(g) for g in grouped.groups.keys()}
            return d
        elif self.pl_df is not None:
            return self.pl_df.group_by(columns)

    def get_nan_mask(self, column: str) -> List[bool]:
        if self.pd_df is not None:
            return self.pd_df[column].isna()  # .to_list()
        elif self.pl_df is not None:
            return self.pl_df[column].is_nan()  # .to_list()
        return []

    def get_val_mask(self, column: str, value: Any) -> List[bool]:
        if self.pd_df is not None:
            return self.pd_df[column] == value  # .to_list()
        elif self.pl_df is not None:
            return self.pl_df[column] == value  # .to_list()
        return []

    def get_group(self, columns: List[str], key: Tuple[Any]) -> Any:
        if self.pd_df is not None:
            grouped = self.pd_df.groupby(columns)
            return grouped.get_group(key)
        elif self.pl_df is not None:
            grouped = self.pl_df.group_by(columns)
            return {k: g for k, g in grouped}[key]
        raise ValueError("Cannot group empty DataFrame")

    def concat(self, other: "DataFrameWrapper") -> None:
        if self.pd_df is not None:
            self.pd_df = pd.concat([self.pd_df, other.pd_df], axis=1)
            self._update()
            return
        elif self.pl_df is not None:
            self.pl_df = pl.concat([self.pl_df, other.pl_df], how="horizontal")
            self._update()
            return
        raise ValueError("Cannot concatenate empty DataFrame")

    def drop_columns(self, columns: List[str]) -> None:
        if self.pd_df is not None:
            self.pd_df = self.pd_df.drop(columns=columns)
            self._update()
            return
        elif self.pl_df is not None:
            self.pl_df = self.pl_df.drop(columns)
            self._update()
            return
        raise ValueError("Cannot drop columns from empty DataFrame")

    def __getitem__(self, key: str) -> "DataFrameWrapper":
        if self.pd_df is not None:
            return DataFrameWrapper(pandas=pd.DataFrame(self.pd_df[key]))
        elif self.pl_df is not None:
            return DataFrameWrapper(polars=pl.DataFrame(self.pl_df[key]))
        raise ValueError("Cannot index empty DataFrame")

    def __setitem__(self, key: str, value: Union[pd.Series, pl.Series]) -> None:
        if self.pd_df is not None:
            self.pd_df[key] = value
            self._update()
            return
        elif self.pl_df is not None:
            self.pl_df = self.pl_df.with_columns(value.alias(key))
            self._update()
            return
        raise ValueError("Cannot index empty DataFrame")

    def apply_udf(self, udf: Callable) -> Union[pd.Series, pl.Series]:
        if self.pd_df is not None:
            return pd.Series(udf(self.pd_df))
        elif self.pl_df is not None:
            return self.pl_df.map_rows(udf)["map"]
        raise ValueError("Cannot apply UDFs to empty DataFrame")

    def apply_type_udf(self, udf: Callable) -> Union[pd.Series, pl.Series]:
        if self.pd_df is not None:
            return pd.Series(udf(self.pd_df[self.pd_df.columns[0]]))
        elif self.pl_df is not None:
            return pl.Series(self.pl_df[self.pl_df.columns[0]].map_elements(udf))
        raise ValueError("Cannot apply UDFs to empty DataFrame")

    def apply_multicolumn_udf(self, udf: Callable) -> "DataFrameWrapper":
        if self.pd_df is not None:
            return DataFrameWrapper(pandas=udf(self.pd_df))
        elif self.pl_df is not None:
            return DataFrameWrapper(polars=udf(self.pl_df))
        raise ValueError("Cannot apply UDFs to empty DataFrame")

    def rename(self, columns: Dict[str, str]) -> None:
        if self.pd_df is not None:
            self.pd_df = self.pd_df.rename(columns=columns)
            self._update()
            return
        elif self.pl_df is not None:
            self.pl_df = self.pl_df.rename(columns)
            self._update()
            return
        raise ValueError("Cannot rename an empty DataFrame")
