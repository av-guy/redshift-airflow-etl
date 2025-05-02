"""Operators to load data from S3 to Redshift."""

from typing import Tuple, Dict, Any

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """Operator to load data from S3 to Redshift.

    Parameters
    ----------
    db_connection_id : str
        The ID of the database connection to use.
    bucket_name : str
        The name of the S3 bucket.
    iam_role : str
        The IAM role ARN.
    copy_table_stmt : str
        The SQL statement to copy data from S3 to Redshift.
    region_name : str, optional
        The AWS region for the S3 bucket. Defaults to "us-west-2".

    Methods
    -------
    execute(self, context: Dict[str, Any]) -> None
        Execute the stage to Redshift operation.
    """

    ui_color = "#358140"

    def __init__(self, **kwargs: Dict[str, Any]):
        self._db_connection_id = kwargs.pop("db_connection_id", None)
        self._bucket_name = kwargs.pop("bucket_name", None)
        self._iam_role = kwargs.pop("iam_role", None)
        self._copy_table_stmt = kwargs.pop("copy_table_stmt", None)
        self._region_name = kwargs.pop("region_name", None)
        self._json_format = kwargs.pop("json_format", None)

        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Execute the stage to Redshift operation.

        s3://{}/log_json_path.json

        Parameters
        ----------
        context : Dict[str, Any]
            The Airflow execution context containing information about the
            current execution.
        """
        self.log.info("Storing data from S3 to Redshift...")
        self.log.debug("Using context: %s", context)

        db_hook = PostgresHook(postgres_conn_id=self._db_connection_id)

        fmt_copy = self._copy_table_stmt.format(
            bucket=self._bucket_name,
            iam_role=self._iam_role,
            json_format=self._json_format,
            region=self._region_name,
        )

        self.log.info("Executing SQL: %s", fmt_copy)
        db_hook.run(fmt_copy, autocommit=True)

        self.log.info("Data stored to Redshift successfully.")
