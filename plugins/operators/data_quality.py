"""Data quality check operator and related"""

from typing import Tuple, List, Any, Dict

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class DataQualityCheckError(Exception):
    """Raised when data quality check fails"""


class DataQualityOperator(BaseOperator):
    """Basic data quality check operator. Checks the count of rows in each table.

    Parameters
    ----------
    db_connection_id : str
        The ID of the Postgres connection to use.
    table_names : List[str]
        The names of the tables to check.
    connection_type : str, optional
        The type of database connection to use. Defaults to "Redshift".

    Methods
    -------
    execute(self, context: Dict[str, Any]) -> None
        Executes the data quality check operation.
    """

    ui_color = "#89DA59"

    def __init__(self, **kwargs: Dict[str, Any]):
        self._db_connection_id = kwargs.pop("db_connection_id", None)
        self._table_names = kwargs.pop("table_names", [])
        self._connection_type = kwargs.pop("connection_type", "Redshift")
        self._data_quality_sql = kwargs.pop("data_quality_sql", None)

        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Execute the data quality check operation.

        Parameters
        ----------
        context : Dict[str, Any]
            The Airflow execution context containing information about the
            current execution.
        """
        self.log.info("Checking data quality for %s...", self._connection_type)
        self.log.debug("Using context: %s", context)

        db_hook = PostgresHook(postgres_conn_id=self._db_connection_id)

        sql = (
            self._data_quality_sql
            if self._data_quality_sql
            else "SELECT COUNT(*) FROM {}"
        )

        for table_name in self._table_names:
            self.log.info("Checking data quality in table %s...", table_name)
            query = sql.format(table_name)
            result = db_hook.get_first(query)

            if result[0] == 0:
                self.log.error("Data quality check failed in table %s.", table_name)
                raise DataQualityCheckError(
                    f"Data quality check failed in table {table_name}."
                )

            self.log.info("Data quality check passed in table %s.", table_name)

        self.log.info("Data quality check successful for %s...", self._connection_type)
