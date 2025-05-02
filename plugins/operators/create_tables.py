"""Operators used to create tables in Redshift"""

from typing import Dict, Any

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class CreateTablesOperator(BaseOperator):
    """Operator used to create tables in Redshift

    Parameters
    ----------
    db_connection_id : str
        The ID of the database connection to use.
    create_table_dict : Dict[str, str]
        A dictionary mapping table names to their SQL CREATE TABLE statements.
    connection_type : str, optional
        The type of database connection to use. Defaults to "Redshift".

    Methods
    -------
    execute(self, context: Dict[str, Any]) -> None
        Execute the create tables operation.
    """

    ui_color = "#80BD9E"

    def __init__(self, **kwargs: Dict[str, Any]):
        self._db_connection_id = kwargs.pop("db_connection_id")
        self._create_table_stmts = kwargs.pop("create_table_stmts")
        self._drop_table_stmts = kwargs.pop("drop_table_stmts", [])
        self._connection_type = kwargs.pop("connection_type", "Redshift")

        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Execute the create tables operation.

        Parameters
        ----------
        context : Dict[str, Any]
            The Airflow execution context containing information about the
            current execution.
        """
        self.log.debug("Using context: %s", context)

        db_hook = PostgresHook(postgres_conn_id=self._db_connection_id)

        self.log.info("Creating tables in %s...", self._connection_type)

        for sql in self._drop_table_stmts:
            db_hook.run(sql, autocommit=True)

        for sql in self._create_table_stmts:
            db_hook.run(sql, autocommit=True)

        self.log.info("Tables created successfully in %s...", self._connection_type)
