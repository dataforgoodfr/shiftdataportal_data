from typing import List, Any

import pandas as pd
import psycopg2
from psycopg2 import sql

from exceptions import ColumnNamesError, NumberColumnsError


def connect_to_database(account_type: str):
    """
    Connects to the PostgreSQL database by reading a file in which the accesses are stored.

    The read file has the following structure:

    │  account_type      │     user      │  password    │ database_name │ host │ port │
    │  read-access       │ johndoe       │              │               │      │      │
    │  read-write-access │ janedoe       │              │               │      │      │

    To have access of the database, ask the team.

    :param account_type: either 'read-access' or 'read-write-access'
    :return: the database connector
    """
    filepath = "../database_access.csv"
    database_access = pd.read_csv(filepath, header=0, index_col="account_type")

    return psycopg2.connect(
        database=database_access.loc[account_type, "database_name"],
        user=database_access.loc[account_type, "user"],
        password=database_access.loc[account_type, "password"],
        host=database_access.loc[account_type, "host"],
        port=database_access.loc[account_type, "port"],
    )


def truncate_and_insert(db_connector: Any, df: pd.DataFrame, schema: str, table_name: str):
    """
    Overwrites the destination table with the source data contained in the dataframe.
    It is done by truncating (or emptying) the destination table and then inserting values from the
    source data into the destination table.

    When using this function, make sure to provide the right destination table.

    :param db_connector: PostgreSQL database connector
    :param df: Dataframe containing the source data
    :param schema: database schema in which the destination table is stored, generally is 'public'
    :param table_name: name of the destination table
    """
    db_cursor = db_connector.cursor()
    try:
        db_cursor.execute(
            sql.SQL("TRUNCATE {table};").format(table=sql.Identifier(schema, table_name))
        )
        table_column_names = _get_column_names_from_table(
            db_cursor, schema=schema, table_name=table_name
        )
        df_column_names = df.columns.to_list()
        _check_columns(table_column_names, df_column_names)
        for index, row in df.iterrows():
            db_cursor.execute(
                sql.SQL(
                    "INSERT INTO {table} ({columns})"
                    "VALUES ({placeholders});"
                ).format(
                    table=sql.Identifier(schema, table_name),
                    columns=sql.SQL(", ").join(
                        [sql.Identifier(table_column_name) for table_column_name in table_column_names]
                    ),
                    placeholders=sql.SQL(", ").join(sql.Placeholder() * len(table_column_names)),
                ),
                tuple([row[df_column_name] for df_column_name in df_column_names])
            )
        db_connector.commit()
        print(f"Table {schema}.{table_name} truncated and populated with the dataframe.")
    except Exception:
        db_connector.rollback()
        raise
    finally:
        db_cursor.close()


def _get_column_names_from_table(db_cursor: Any, schema: str, table_name: str) -> List[str]:
    """Returns the column names of a table from the PostgreSQL database."""
    db_cursor.execute(
        sql.SQL("SELECT * FROM {table} LIMIT 1;").format(table=sql.Identifier(schema, table_name))
    )
    return [column.name for column in list(db_cursor.description)]


def _check_columns(table_columns: List[str], df_columns: List[str]):
    """Performs checks on the columns present in the source and in the destination."""
    if len(table_columns) != len(df_columns):
        raise NumberColumnsError(
            "The number of columns of your dataframe must be the same as the number of columns of the table "
            "in which you want to write data."
        )
    elif sorted(table_columns) != sorted(df_columns):
        raise ColumnNamesError(
            "The column names between your dataframe and the table in which you want to write data must be the same."
        )
