# to regenerate this from scratch, run scripts/regenerate_python_stubs.sh .
# be warned - currently there are still tweaks needed after this file is
# generated. These should be annotated with a comment like
# # stubgen override
# to help the sanity of maintainers.
import typing
# stubgen override - missing import of Set
from typing import Any, ClassVar, Set, Optional

from typing import overload, Dict, List
import pandas
# stubgen override - unfortunately we need this for version checks
import sys
import fsspec
import pyarrow.lib
import polars
# stubgen override - This should probably not be exposed
#_clean_default_connection: Any
apilevel: str
comment: token_type
default_connection: DuckDBPyConnection
identifier: token_type
keyword: token_type
numeric_const: token_type
operator: token_type
paramstyle: str
string_const: token_type
threadsafety: int
__standard_vector_size__: int

__interactive__: bool
__jupyter__: bool

class BinderException(ProgrammingError): ...

class CastException(DataError): ...

class CatalogException(ProgrammingError): ...

class ConnectionException(OperationalError): ...

class ConstraintException(IntegrityError): ...

class ConversionException(DataError): ...

class DataError(Error): ...

class DuckDBPyConnection:
    def __init__(self, *args, **kwargs) -> None: ...
    def append(self, table_name: str, df: pandas.DataFrame) -> DuckDBPyConnection: ...
    def arrow(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def begin(self) -> DuckDBPyConnection: ...
    def close(self) -> None: ...
    def commit(self) -> DuckDBPyConnection: ...
    def cursor(self) -> DuckDBPyConnection: ...
    def df(self) -> pandas.DataFrame: ...
    def duplicate(self) -> DuckDBPyConnection: ...
    def execute(self, query: str, parameters: object = ..., multiple_parameter_sets: bool = ...) -> DuckDBPyConnection: ...
    def executemany(self, query: str, parameters: object = ...) -> DuckDBPyConnection: ...
    def fetch_arrow_table(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def fetch_df(self, *args, **kwargs) -> pandas.DataFrame: ...
    def fetch_df_chunk(self, *args, **kwargs) -> pandas.DataFrame: ...
    def fetch_record_batch(self, chunk_size: int = ...) -> pyarrow.lib.RecordBatchReader: ...
    def fetchall(self) -> list: ...
    def fetchdf(self, *args, **kwargs) -> pandas.DataFrame: ...
    def fetchmany(self, size: int = ...) -> list: ...
    def fetchnumpy(self) -> dict: ...
    def fetchone(self) -> typing.Optional[tuple]: ...
    def from_arrow(self, arrow_object: object) -> DuckDBPyRelation: ...
    def read_json(
        self,
        file_name: str,
        columns: Optional[Dict[str,str]] = None,
        sample_size: Optional[int] = None,
        maximum_depth: Optional[int] = None
    ) -> DuckDBPyRelation: ...
    def read_csv(
        self,
        name: str,
        header: Optional[bool | int] = None,
        compression: Optional[str] = None,
        sep: Optional[str] = None,
        delimiter: Optional[str] = None,
        dtype: Optional[Dict[str, str] | List[str]] = None,
        na_values: Optional[str] = None,
        skiprows: Optional[int] = None,
        quotechar: Optional[str] = None,
        escapechar: Optional[str] = None,
        encoding: Optional[str] = None,
        parallel: Optional[bool] = None,
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        sample_size: Optional[int] = None,
        all_varchar: Optional[bool] = None,
        normalize_names: Optional[bool] = None,
        filename: Optional[bool] = None,
    ) -> DuckDBPyRelation: ...
    def from_csv_auto(
        self,
        name: str,
        header: Optional[bool | int] = None,
        compression: Optional[str] = None,
        sep: Optional[str] = None,
        delimiter: Optional[str] = None,
        dtype: Optional[Dict[str, str] | List[str]] = None,
        na_values: Optional[str] = None,
        skiprows: Optional[int] = None,
        quotechar: Optional[str] = None,
        escapechar: Optional[str] = None,
        encoding: Optional[str] = None,
        parallel: Optional[bool] = None,
        date_format: Optional[str] = None,
        timestamp_format: Optional[str] = None,
        sample_size: Optional[int] = None,
        all_varchar: Optional[bool] = None,
        normalize_names: Optional[bool] = None,
        filename: Optional[bool] = None,
    ) -> DuckDBPyRelation: ...
    def from_df(self, df: pandas.DataFrame = ...) -> DuckDBPyRelation: ...
    @overload
    def read_parquet(self, file_glob: str, binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ...) -> DuckDBPyRelation: ...
    @overload
    def read_parquet(self, file_globs: List[str], binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ...) -> DuckDBPyRelation: ...
    @overload
    def from_parquet(self, file_glob: str, binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ...) -> DuckDBPyRelation: ...
    @overload
    def from_parquet(self, file_globs: List[str], binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ...) -> DuckDBPyRelation: ...
    def from_query(self, query: str, alias: str = ...) -> DuckDBPyRelation: ...
    def from_substrait(self, proto: bytes) -> DuckDBPyRelation: ...
    def get_substrait(self, query: str) -> DuckDBPyRelation: ...
    def get_substrait_json(self, query: str) -> DuckDBPyRelation: ...
    def from_substrait_json(self, json: str) -> DuckDBPyRelation: ...
    def get_table_names(self, query: str) -> Set[str]: ...
    def install_extension(self, *args, **kwargs) -> None: ...
    def list_filesystems(self) -> list: ...
    def load_extension(self, extension: str) -> None: ...
    def pl(self, chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> polars.DataFrame: ...
    def torch(self, connection: DuckDBPyConnection = ...) -> dict: ...
    def query(self, query: str, alias: str = ...) -> DuckDBPyRelation: ...
    def register(self, view_name: str, python_object: object) -> DuckDBPyConnection: ...
    def register_filesystem(self, filesystem: fsspec.AbstractFileSystem) -> None: ...
    def rollback(self) -> DuckDBPyConnection: ...
    def sql(self, query: str, alias: str = ...) -> DuckDBPyRelation: ...
    def table(self, table_name: str) -> DuckDBPyRelation: ...
    def table_function(self, name: str, parameters: object = ...) -> DuckDBPyRelation: ...
    def unregister(self, view_name: str) -> DuckDBPyConnection: ...
    def unregister_filesystem(self, name: str) -> None: ...
    def values(self, values: object) -> DuckDBPyRelation: ...
    def view(self, view_name: str) -> DuckDBPyRelation: ...
    def __enter__(self) -> DuckDBPyConnection: ...
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool: ...
    @property
    def description(self) -> typing.Optional[list]: ...

class DuckDBPyRelation:
    def close(self) -> None: ...
    def __init__(self, *args, **kwargs) -> None: ...
    def abs(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def aggregate(self, aggr_expr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def apply(self, function_name: str, function_aggr: str, group_expr: str = ..., function_parameter: str = ..., projected_columns: str = ...) -> DuckDBPyRelation: ...
    def arrow(self, batch_size: int = ...) -> pyarrow.lib.Table: ...
    def count(self, count_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def create(self, table_name: str) -> None: ...
    def create_view(self, view_name: str, replace: bool = ...) -> DuckDBPyRelation: ...
    def cummax(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cummin(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cumprod(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def cumsum(self, aggregation_columns: str) -> DuckDBPyRelation: ...
    def describe(self) -> DuckDBPyRelation: ...
    def df(self, *args, **kwargs) -> pandas.DataFrame: ...
    def distinct(self) -> DuckDBPyRelation: ...
    def except_(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def execute(self, *args, **kwargs) -> DuckDBPyRelation: ...
    def explain(self) -> str: ...
    def fetchall(self) -> list: ...
    def fetchmany(self, size: int = ...) -> list: ...
    def fetchnumpy(self) -> dict: ...
    def fetchone(self) -> typing.Optional[tuple]: ...
    def fetchdf(self, *args, **kwargs) -> Any: ...
    def fetch_arrow_table(self, chunk_size: int = ...) -> pyarrow.lib.Table: ...
    def filter(self, filter_expr: str) -> DuckDBPyRelation: ...
    def insert(self, values: object) -> None: ...
    def insert_into(self, table_name: str) -> None: ...
    def intersect(self, other_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def join(self, other_rel: DuckDBPyRelation, condition: str, how: str = ...) -> DuckDBPyRelation: ...
    def kurt(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def limit(self, n: int, offset: int = ...) -> DuckDBPyRelation: ...
    def mad(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def map(self, map_function: function) -> DuckDBPyRelation: ...
    def max(self, max_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def mean(self, mean_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def median(self, median_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def min(self, min_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def mode(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def order(self, order_expr: str) -> DuckDBPyRelation: ...
    def pl(self, chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> polars.DataFrame: ...
    def torch(self, connection: DuckDBPyConnection = ...) -> dict: ...
    def prod(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def project(self, project_expr: str) -> DuckDBPyRelation: ...
    def quantile(self, q: str, quantile_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def query(self, virtual_table_name: str, sql_query: str) -> DuckDBPyRelation: ...
    def record_batch(self, batch_size: int = ...) -> pyarrow.lib.RecordBatchReader: ...
    def fetch_arrow_reader(self, batch_size: int = ...) -> pyarrow.lib.RecordBatchReader: ...
    def sem(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def set_alias(self, alias: str) -> DuckDBPyRelation: ...
    def show(self) -> None: ...
    def skew(self, aggregation_columns: str, group_columns: str = ...) -> DuckDBPyRelation: ...
    def std(self, std_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def sum(self, sum_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def to_arrow_table(self, batch_size: int = ...) -> pyarrow.lib.Table: ...
    def to_df(self, *args, **kwargs) -> pandas.DataFrame: ...
    def union(self, union_rel: DuckDBPyRelation) -> DuckDBPyRelation: ...
    def unique(self, unique_aggr: str) -> DuckDBPyRelation: ...
    def value_counts(self, value_counts_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def var(self, var_aggr: str, group_expr: str = ...) -> DuckDBPyRelation: ...
    def to_parquet(
        self,
        file_name: str,
        compression: Optional[str]
    ) -> None: ...
    def write_parquet(
        self,
        file_name: str,
        compression: Optional[str]
    ) -> None: ...
    def to_csv(
        self,
        file_name: str,
        sep: Optional[str],
        na_rep: Optional[str],
        header: Optional[bool],
        quotechar: Optional[str],
        escapechar: Optional[str],
        date_format: Optional[str],
        timestamp_format: Optional[str],
        quoting: Optional[str | int],
        encoding: Optional[str],
        compression: Optional[str]
    ) -> None: ...
    def write_csv(
        self,
        file_name: str,
        sep: Optional[str],
        na_rep: Optional[str],
        header: Optional[bool],
        quotechar: Optional[str],
        escapechar: Optional[str],
        date_format: Optional[str],
        timestamp_format: Optional[str],
        quoting: Optional[str | int],
        encoding: Optional[str],
        compression: Optional[str]
    ) -> None: ...
    def __len__(self) -> int: ...
    @property
    def alias(self) -> str: ...
    @property
    def columns(self) -> list: ...
    @property
    def dtypes(self) -> list: ...
    @property
    def description(self) -> list: ...
    @property
    def shape(self) -> tuple: ...
    @property
    def type(self) -> str: ...
    @property
    def types(self) -> list: ...

class Error(Exception): ...

class FatalException(Error): ...

class IOException(OperationalError): ...

class IntegrityError(Error): ...

class InternalError(Error): ...

class InternalException(InternalError): ...

class InterruptException(Error): ...

class InvalidInputException(ProgrammingError): ...

class InvalidTypeException(ProgrammingError): ...

class NotImplementedException(NotSupportedError): ...

class NotSupportedError(Error): ...

class OperationalError(Error): ...

class OutOfMemoryException(OperationalError): ...

class OutOfRangeException(DataError): ...

class ParserException(ProgrammingError): ...

class PermissionException(Error): ...

class ProgrammingError(Error): ...

class SequenceException(Error): ...

class SerializationException(OperationalError): ...

class StandardException(Error): ...

class SyntaxException(ProgrammingError): ...

class TransactionException(OperationalError): ...

class TypeMismatchException(DataError): ...

class ValueOutOfRangeException(DataError): ...

class Warning(Exception): ...

class token_type:
    # stubgen override - these make mypy sad
    #__doc__: ClassVar[str] = ...  # read-only
    #__members__: ClassVar[dict] = ...  # read-only
    __entries: ClassVar[dict] = ...
    comment: ClassVar[token_type] = ...
    identifier: ClassVar[token_type] = ...
    keyword: ClassVar[token_type] = ...
    numeric_const: ClassVar[token_type] = ...
    operator: ClassVar[token_type] = ...
    string_const: ClassVar[token_type] = ...
    def __init__(self, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __getstate__(self) -> int: ...
    def __hash__(self) -> int: ...
    # stubgen override - pybind only puts index in python >= 3.8: https://github.com/EricCousineau-TRI/pybind11/blob/54430436/include/pybind11/pybind11.h#L1789
    if sys.version_info >= (3, 7):
        def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    def __ne__(self, other: object) -> bool: ...
    def __setstate__(self, state: int) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...
    @property
    # stubgen override - this gets removed by stubgen but it shouldn't
    def __members__(self) -> object: ...

def aggregate(df: pandas.DataFrame, aggr_expr: str, group_expr: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def alias(df: pandas.DataFrame, alias: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def connect(database: str = ..., read_only: bool = ..., config: dict = ...) -> DuckDBPyConnection: ...
def distinct(df: pandas.DataFrame, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def filter(df: pandas.DataFrame, filter_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_substrait_json(jsonm: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def limit(df: pandas.DataFrame, n: int, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def order(df: pandas.DataFrame, order_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def project(df: pandas.DataFrame, project_expr: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def write_csv(df: pandas.DataFrame, file_name: str, connection: DuckDBPyConnection = ...) -> None: ...
def read_json(
    file_name: str,
    columns: Optional[Dict[str,str]] = None,
    sample_size: Optional[int] = None,
    maximum_depth: Optional[int] = None,
    connection: DuckDBPyConnection = ...
) -> DuckDBPyRelation: ...
def read_csv(
    name: str,
    header: Optional[bool | int] = None,
    compression: Optional[str] = None,
    sep: Optional[str] = None,
    delimiter: Optional[str] = None,
    dtype: Optional[Dict[str, str] | List[str]] = None,
    na_values: Optional[str] = None,
    skiprows: Optional[int] = None,
    quotechar: Optional[str] = None,
    escapechar: Optional[str] = None,
    encoding: Optional[str] = None,
    parallel: Optional[bool] = None,
    date_format: Optional[str] = None,
    timestamp_format: Optional[str] = None,
    sample_size: Optional[int] = None,
    all_varchar: Optional[bool] = None,
    normalize_names: Optional[bool] = None,
    filename: Optional[bool] = None,
    connection: DuckDBPyConnection = ...
) -> DuckDBPyRelation: ...
def from_csv_auto(
    name: str,
    header: Optional[bool | int] = None,
    compression: Optional[str] = None,
    sep: Optional[str] = None,
    delimiter: Optional[str] = None,
    dtype: Optional[Dict[str, str] | List[str]] = None,
    na_values: Optional[str] = None,
    skiprows: Optional[int] = None,
    quotechar: Optional[str] = None,
    escapechar: Optional[str] = None,
    encoding: Optional[str] = None,
    parallel: Optional[bool] = None,
    date_format: Optional[str] = None,
    timestamp_format: Optional[str] = None,
    sample_size: Optional[int] = None,
    all_varchar: Optional[bool] = None,
    normalize_names: Optional[bool] = None,
    filename: Optional[bool] = None,
    connection: DuckDBPyConnection = ...
) -> DuckDBPyRelation: ...

def append(table_name: str, df: pandas.DataFrame, connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def arrow(chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> pyarrow.lib.Table: ...
def begin(connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def close(connection: DuckDBPyConnection = ...) -> None: ...
def commit(connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def cursor(connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def df(connection: DuckDBPyConnection = ...) -> pandas.DataFrame: ...
def description(connection: DuckDBPyConnection = ...) -> typing.Optional[list]: ...
def duplicate(connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def execute(query: str, parameters: object = ..., multiple_parameter_sets: bool = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def executemany(query: str, parameters: object = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def fetch_arrow_table(chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> pyarrow.lib.Table: ...
def fetch_df(*args, connection: DuckDBPyConnection = ..., **kwargs) -> pandas.DataFrame: ...
def fetch_df_chunk(*args, connection: DuckDBPyConnection = ..., **kwargs) -> pandas.DataFrame: ...
def fetch_record_batch(chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> pyarrow.lib.RecordBatchReader: ...
def fetchall(connection: DuckDBPyConnection = ...) -> list: ...
def fetchdf(*args, connection: DuckDBPyConnection = ..., **kwargs) -> pandas.DataFrame: ...
def fetchmany(size: int = ..., connection: DuckDBPyConnection = ...) -> list: ...
def fetchnumpy(connection: DuckDBPyConnection = ...) -> dict: ...
def fetchone(connection: DuckDBPyConnection = ...) -> typing.Optional[tuple]: ...
def from_arrow(arrow_object: object, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_df(df: pandas.DataFrame = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def read_parquet(file_glob: str, binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def read_parquet(file_globs: List[str], binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def from_parquet(file_glob: str, binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
@overload
def from_parquet(file_globs: List[str], binary_as_string: bool = ..., *, file_row_number: bool = ..., filename: bool = ..., hive_partitioning: bool = ..., union_by_name: bool = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_query(query: str, alias: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def from_substrait(proto: bytes, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def get_substrait(query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def get_substrait_json(query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def get_table_names(query: str, connection: DuckDBPyConnection = ...) -> Set[str]: ...
def install_extension(*args, connection: DuckDBPyConnection = ..., **kwargs) -> None: ...
def list_filesystems(connection: DuckDBPyConnection = ...) -> list: ...
def load_extension(extension: str, connection: DuckDBPyConnection = ...) -> None: ...
def pl(chunk_size: int = ..., connection: DuckDBPyConnection = ...) -> polars.DataFrame: ...
def torch(connection: DuckDBPyConnection = ...) -> dict: ...
def query(query: str, alias: str = 'query_relation', connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def register(view_name: str, python_object: object, connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def register_filesystem(filesystem: fsspec.AbstractFileSystem, connection: DuckDBPyConnection = ...) -> None: ...
def rollback(connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def sql(query: str, alias: str = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def table(table_name: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def table_function(name: str, parameters: object = ..., connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def unregister(view_name: str, connection: DuckDBPyConnection = ...) -> DuckDBPyConnection: ...
def query_df(df: pandas.DataFrame, virtual_table_name: str, sql_query: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def unregister_filesystem(name: str, connection: DuckDBPyConnection = ...) -> None: ...
def tokenize(query: str) -> list: ...
def values(values: object, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
def view(view_name: str, connection: DuckDBPyConnection = ...) -> DuckDBPyRelation: ...
