from typing import Tuple, Dict, Any

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    """Operator to load dimension data to Redshift.

    Parameters
    ----------
    db_connection_id : str
        The ID of the database connection to use.
    insert_dim_stmt : str
        The SQL statement to insert dimension data into Redshift.
    connection_type : str, optional
        The type of database connection to use. Defaults to "Redshift".

    Methods
    -------
    execute(self, context: Dict[str, Any]) -> None
        Execute the load dimension operation.
    """

    ui_color = "#80BD9E"

    def __init__(self, **kwargs: Dict[str, Any]):
        self._db_connection_id = kwargs.pop("db_connection_id", None)
        self._insert_dim_stmt = kwargs.pop("insert_dim_stmt", None)
        self._connection_type = kwargs.pop("connection_type", "Redshift")

        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Execute the load dimension operation.

        Parameters
        ----------
        context : Dict[str, Any]
            The Airflow execution context containing information about the
            current execution.
        """
        self.log.info("Loading dimension data to %s...", self._connection_type)
        self.log.debug("Using context: %s", context)

        db_hook = PostgresHook(postgres_conn_id=self._db_connection_id)
        db_hook.run(self._insert_dim_stmt, autocommit=True)

        self.log.info(
            "Dimension data loaded successfully to %s.", self._connection_type
        )
