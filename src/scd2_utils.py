"""Module to create SCD2. Works based on assuming a full load."""
import pytz
import pyspark.sql as p
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime as dt
from functools import partial
import typing as t
from delta.tables import DeltaTable

META_DATA_HASH_COL = 'meta_data_hash'
META_ID_HASH_COL = 'meta_id_hash'
META_DT_VALID_TO_COL = 'meta_dt_valid_to'
META_DT_VALID_FROM_COL = 'meta_dt_valid_from'
VALID_DATE = '9999-12-31'


def spark_session():
    return SparkSession.builder.getOrCreate()


def add_hash_key_column(df: p.DataFrame, cols_to_use: t.List[str], col_name: str = "hash_key"):
    """Creates hashed key column.

    Parameters
    ----------
    df : p.DataFrame
        PySpark dataframe to add hash key column to.
    cols_to_use : t.List[str]
        Columns to use for hashing.

    Returns
    -------
    p.DataFrame
        PySpark dataframe with hash key column.
    """
    hash_function = F.sha2(F.concat_ws(
        "||", *df.select(cols_to_use).columns), 256)
    df = df.withColumn(col_name, F.lit(hash_function))
    return df


def _check_required_fields(fields_to_check: list, fields_expected: list):
    """Assert whether required fields are in a given list.

    Parameters
    ----------
    fields_to_check : list
        List of items to check.
    fields_expected : list
        List of expected fields.
    """
    # check if all fields are available as expected, returns boolean list
    checked_fields = check_elements_in_list(fields_to_check, fields_expected)
    # reverse boolean list
    reverse_checked_fields = [not elem for elem in checked_fields]
    missing_fields = filter_list_by_boolean(
        fields_expected, reverse_checked_fields)
    assert all(
        checked_fields), f"Missing field(s) are {missing_fields}. Fields to check contains {fields_to_check}"


def check_elements_in_list(elements_to_check: list, elements_expected: list) -> t.List[bool]:
    """Check if a number of expected elements are in a list as expected.

    Parameters
    ----------
    elements_to_check : list
        List of elements to check.
    elements_expected : list
        List of elements to be expected to be in.

    Returns
    -------
    t.List[bool]
        Boolean list of length of `elements_expected` to indicate if element in elements_to_check.
    """
    return [elem in elements_to_check for elem in elements_expected]


def filter_list_by_boolean(list_to_filter: list, boolean_list: t.List[bool]) -> list:
    """Filter list by boolean list.

    Parameters
    ----------
    list_to_filter : list
        List to filter on.
    boolean_list : t.List[bool]
        Boolean mask to filter out list.

    Returns
    -------
    list
        Filtered list.
    """
    return [elem for (elem, truth) in zip(list_to_filter, boolean_list) if truth]


def create_meta_columns(df: p.DataFrame, id_cols: t.List[str], data_cols: t.List[str] = None):
    """Create meta columns.

    Parameters
    ----------
    df : p.DataFrame
        PySpark Dataframe.
    id_cols : t.List[str]
        Columns to use for meta_id_hash.
    data_cols : t.List[str], optional
        Data columns to hash, by default None (Uses all columns except `meta_` and id_columns).

    Returns
    -------
    p.DataFrame
        PySpark Dataframe with meta_id and data_hash columns.
    """
    _check_required_fields(df.columns, id_cols)
    if data_cols is None:
        data_cols = list(set(df.columns) - set(id_cols) -
                         set([cols for cols in df.columns if "meta" in cols]))
    df = (df
          .transform(partial(add_hash_key_column, cols_to_use=id_cols, col_name=META_ID_HASH_COL))
          .transform(partial(add_hash_key_column, cols_to_use=data_cols, col_name=META_DATA_HASH_COL))
          )
    return df


def add_meta_valid_cols(df: p.DataFrame, from_date: str, to_date: str = VALID_DATE):
    """Add validity columns, e.g. meta_valid_to and meta_dt_valid_from.

    Parameters
    ----------
    df : p.DataFrame
        Pyspark Dataframe.
    from_date : str
        Date to set the start of validity.
    to_date : str, optional
        Date to set the end of validity, by default VALID_DATE=='9999-12-31'

    Returns
    -------
    p.DataFrame
        Pyspark Dataframe with validity columns.
    """
    df = (df
          .withColumn(META_DT_VALID_TO_COL, F.lit(to_date))
          .withColumn(META_DT_VALID_FROM_COL, F.lit(from_date))
          )
    return df


def column_suffix_rename(df: p.DataFrame, suffix: str, keys_list: t.List[str], add_suffix: bool = True):
    """Add or remove specified suffixes.

    Parameters
    ----------
    df : p.DataFrame
        Pyspark Dataframe.
    suffix : str
        Suffix.
    keys_list : t.List[str]
        List of column names to exclude from suffixing
    add_suffix : bool, optional
        Whether to add or remove suffixes, by default True (adds suffix). If False, specified suffix is removed.

    Returns
    -------
    p.DataFrame
        Pyspark Dataframe with renamed columns.
    """
    if add_suffix:
        for column in set(df.columns) - set(keys_list):
            df = df.withColumnRenamed(column, column + suffix)
    else:
        for column in [cols for cols in df.columns if f"{suffix}" in cols]:
            df = df.withColumnRenamed(column, column.replace(suffix, ""))
    return df


def get_valid_rows(df: p.DataFrame):
    """Get valid rows.

    Parameters
    ----------
    df : p.DataFrame
        Pyspark Dataframe.

    Returns
    -------
    p.DataFrame
        Pyspark Dataframe with valid rows.
    """
    return df.filter(F.col(META_DT_VALID_TO_COL) == VALID_DATE)


def get_closed_rows(df: p.DataFrame):
    """Get closed rows.

    Parameters
    ----------
    df : p.DataFrame
        Pyspark Dataframe.

    Returns
    -------
    p.DataFrame
        Pyspark Dataframe with closed rows.
    """
    return df.filter(F.col(META_DT_VALID_TO_COL) != VALID_DATE)


def get_new_rows(df: p.DataFrame, valid_rows: p.DataFrame,):
    """Get new rows with a left_anti merge between the new dataframe and the old valid rows..

    Parameters
    ----------
    df : p.DataFrame
        New Dataframe to extract new rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing new rows.
    """
    return df.join(valid_rows, on=META_ID_HASH_COL, how='left_anti')


def get_deleted_rows(df: p.DataFrame, valid_rows: p.DataFrame, closing_date: str = None):
    """Get deleted rows and update meta_valid_to

    Parameters
    ----------
    df : p.DataFrame
        New Dataframe to extract deleted rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.
    closing_date : str, optional
        Date to use to close off the row, by default None (get recent date)

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing deleted rows.
    """
    if closing_date is None:
        closing_date = get_closing_date()
    deleted_rows = valid_rows.join(df, on=META_ID_HASH_COL, how='left_anti')
    deleted_rows = deleted_rows.withColumn(
        META_DT_VALID_TO_COL, F.lit(closing_date))
    return deleted_rows


def get_unchanged_rows(df: p.DataFrame, valid_rows: p.DataFrame, id_cols: t.List[str]):
    """Get rows that have not changed.

    Parameters
    ----------
    df : p.DataFrame
        New Dataframe to extract unchanged rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.
    id_cols : t.List[str]
        List of id columns to join on.

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing unchanged rows.
    """
    valid_rows_rn = column_suffix_rename(
        valid_rows, '_hist', id_cols, add_suffix=True)

    df_rn = column_suffix_rename(df, '_current', id_cols, add_suffix=True)

    unchanged_rows = column_suffix_rename(
        valid_rows_rn
        .join(df_rn, id_cols, how='inner')
        .where(valid_rows_rn[META_DATA_HASH_COL + '_hist'] == df_rn[META_DATA_HASH_COL + '_current'])
        .drop(*[column for column in df_rn.columns if "_current" in column]),
        suffix='_hist', keys_list=id_cols, add_suffix=False
    ).select(valid_rows.columns)
    return unchanged_rows


def get_current_local_datetime(timezone='Europe/Amsterdam') -> str:
    """Get current local datetime.

    Parameters
    ----------
    timezone : str, optional
        Definition of local. Default is 'Europe/Amsterdam'., by default 'Europe/Amsterdam'

    Returns
    -------
    str
        Current local datetime string.
    """
    local_tz = pytz.timezone(timezone)
    utc_dt = dt.datetime.now()
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)


def get_closing_date(date_format: str = "%Y-%m-%d"):
    """Get current date -1 day as closing date.

    Parameters
    ----------
    date_format : str, optional
        Dateformat to use, by default "%Y-%m-%d"
    """
    run_date = get_current_local_datetime().strftime(date_format)
    date = dt.datetime.strptime(run_date, date_format)
    closing_date = (date - dt.timedelta(1)).strftime(date_format)
    return closing_date


def get_rows_changed_closed(valid_rows_rn: p.DataFrame, df_rn: p.DataFrame,
                            valid_rows: p.DataFrame, id_cols: t.List[str],
                            closing_date: str = None):
    """Get "old" changed rows and close entries.

    Parameters
    ----------
    valid_rows_rn : p.DataFrame
        Pyspark Dataframe containing valid rows and renamed columns.
    df_rn : p.DataFrame
        New renamed Dataframe to extract changed rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.
    id_cols : t.List[str]
        List of id columns to join on.
    closing_date : str, optional
        Date to use to close off the row, by default None (get recent date)

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing rows that changed & are closed off.
    """
    if closing_date is None:
        closing_date = get_closing_date()

    rows_changed_closed = column_suffix_rename(
        valid_rows_rn
        .join(df_rn, id_cols, how='inner')
        .where(valid_rows_rn[META_DATA_HASH_COL + '_hist'] != df_rn[META_DATA_HASH_COL + '_current'])
        .drop(*[column for column in df_rn.columns if "_current" in column]),
        suffix='_hist', keys_list=id_cols, add_suffix=False
    ).select(valid_rows.columns)

    rows_changed_closed = rows_changed_closed.withColumn(
        META_DT_VALID_TO_COL, F.lit(closing_date))
    return rows_changed_closed


def get_rows_changed_open(valid_rows_rn: p.DataFrame, df_rn: p.DataFrame,
                          df: p.DataFrame, id_cols: t.List[str]):
    """Get rows that changed but are still open.

    Parameters
    ----------
    valid_rows_rn : p.DataFrame
        Pyspark Dataframe containing valid rows and renamed columns.
    df_rn : p.DataFrame
        New renamed Dataframe to extract changed rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.
    id_cols : t.List[str]
        List of id columns to join on.

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing rows that changed & are open.
    """
    rows_changed_closed = column_suffix_rename(
        df_rn
        .join(valid_rows_rn, id_cols, how='inner')
        .where(valid_rows_rn[META_DATA_HASH_COL + '_hist'] != df_rn[META_DATA_HASH_COL + '_current'])
        .drop(*[column for column in df_rn.columns if "_hist" in column]),
        suffix='_current', keys_list=id_cols, add_suffix=False
    ).select(df.columns)
    return rows_changed_closed


def get_changed_rows(df: p.DataFrame, valid_rows: p.DataFrame, id_cols: t.List[str]):
    """Get changed rows and close old entries.

    Parameters
    ----------
    df : p.DataFrame
        New Dataframe to extract changed rows from.
    valid_rows : p.DataFrame
        Pyspark Dataframe containing valid rows.
    id_cols : t.List[str]
        List of id columns to join on.

    Returns
    -------
    p.DataFrame
        Subset Pyspark Dataframe containing rows that changed.
    """
    valid_rows_rn = column_suffix_rename(
        valid_rows, '_hist', id_cols, add_suffix=True)
    df_rn = column_suffix_rename(df, '_current', id_cols, add_suffix=True)
    rows_changed_closed = get_rows_changed_closed(
        valid_rows_rn, df_rn, valid_rows, id_cols)
    rows_changed_open = get_rows_changed_open(
        valid_rows_rn, df_rn, df, id_cols)
    return (rows_changed_open.select(sorted(rows_changed_open.columns))
            .union(rows_changed_closed.select(sorted(rows_changed_closed.columns))))


def apply_scd2(df_src: p.DataFrame, df_tgt: p.DataFrame,
               id_cols: t.List[str], hash_data_cols: t.List[str],
               date: str = None, create_meta_valid_cols: bool = False):
    """Apply slowly changing dimensions 2. Creates meta columns (meta_id_hash, meta_data_hash, meta_dt_valid_from,
    meta_dt_valid_to), gets valid, new, unchanged, deleted, and changed rows from a Pyspark Dataframe and compares
    it with the target table.

    Parameters
    ----------
    df_src : p.DataFrame
        New Pyspark Dataframe to use to compare to df_tgt.
    df_tgt : p.DataFrame
        Previous target table. Initial load will be done by inserting an empty df_target.
    id_cols : t.List[str]
        List of id columns to join on.
    hash_data_cols : t.List[str]
        Data columns to use to hash.
    date : str, optional
        Date to start validity from, by default None (will take today's date).
    create_meta_valid_cols: bool, optional
        Whether or not to create meta_validity columns. Often not used as those from dwh can be used.

    Returns
    -------
    p.DataFrame
        Returns rows staged for updates. Contains changed, unchanged, deleted, new and valid rows.
    """
    date = get_current_local_datetime().strftime(
        "%Y-%m-%d") if date is None else date
    df_src = create_meta_columns(
        df_src, id_cols=id_cols, data_cols=hash_data_cols)

    if create_meta_valid_cols:
        df_src = add_meta_valid_cols(df_src, from_date=date)

    if df_tgt.count() > 1:
        closed_rows = get_closed_rows(df_tgt)
        valid_rows = get_valid_rows(df_tgt)
        new_rows = get_new_rows(df_src, valid_rows)
        unchanged_rows = get_unchanged_rows(df_src, valid_rows, id_cols)
        deleted_rows = get_deleted_rows(df_src, valid_rows, closing_date=None)
        changed_rows = get_changed_rows(df_src, valid_rows, id_cols)

        staged_updates = (
            unchanged_rows.select(sorted(unchanged_rows.columns))
            .union(new_rows.select(sorted(new_rows.columns)))
            .union(deleted_rows.select(sorted(deleted_rows.columns)))
            .union(changed_rows.select(sorted(changed_rows.columns)))
            .union(closed_rows.select(sorted(closed_rows.columns)))
        )
    else:
        staged_updates = df_src
    return staged_updates
