"""Final project submission"""

# pylint: disable=pointless-statement
# pylint: disable=expression-not-assigned

from datetime import timedelta

import pendulum

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    CreateTablesOperator,
)

import helpers.sql_queries as SQL_QUERIES
import config as CONFIG

default_args = {
    "start_date": pendulum.datetime(2023, 1, 1, tz="UTC"),
    "owner": "av-guy",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None,
    # schedule_interval="0 * * * *",
)
def final_project():
    """Execute the final project DAG based on the provided diagram."""

    begin_execution = DummyOperator(task_id="begin_execution")

    s3_bucket_name = Variable.get("s3_bucket_name")
    iam_role_arn = Variable.get("iam_role_arn")

    create_tables = CreateTablesOperator(
        task_id="create_tables",
        db_connection_id="redshift",
        create_table_stmts=SQL_QUERIES.CREATE_TABLE_STATEMENTS,
        drop_table_stmts=SQL_QUERIES.DROP_TABLE_STATEMENTS,
        connection_type="Redshift",
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        db_connection_id="redshift",
        bucket_name=s3_bucket_name,
        iam_role=iam_role_arn,
        copy_table_stmt=SQL_QUERIES.COPY_STAGING_EVENTS,
        region_name="us-west-2",
        json_format=f"s3://{s3_bucket_name}/log_json_path.json",
    )

    # Have to load from `udacity-dend` because CloudShell will not let me
    # place songs in root directory. It runs out of space.
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        db_connection_id="redshift",
        bucket_name="udacity-dend",
        iam_role=iam_role_arn,
        copy_table_stmt=SQL_QUERIES.COPY_STAGING_SONGS,
        region_name="us-west-2",
        json_format="auto",
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id="load_songplays_fact_table",
        db_connection_id="redshift",
        insert_fact_stmt=SQL_QUERIES.SONGPLAY_TABLE_INSERT,
        connection_type="Redshift",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="load_user_dim_table",
        db_connection_id="redshift",
        insert_dim_stmt=SQL_QUERIES.USER_TABLE_INSERT,
        connection_type="Redshift",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="load_song_dim_table",
        db_connection_id="redshift",
        insert_dim_stmt=SQL_QUERIES.SONG_TABLE_INSERT,
        connection_type="Redshift",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="load_artist_dim_table",
        db_connection_id="redshift",
        insert_dim_stmt=SQL_QUERIES.ARTIST_TABLE_INSERT,
        connection_type="Redshift",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="load_time_dim_table",
        db_connection_id="redshift",
        insert_dim_stmt=SQL_QUERIES.TIME_TABLE_INSERT,
        connection_type="Redshift",
    )

    run_quality_checks = DataQualityOperator(
        task_id="run_quality_checks",
        db_connection_id="redshift",
        table_names=CONFIG.TABLE_NAMES,
        connection_type="Redshift",
        data_quality_sql=SQL_QUERIES.DATA_QUALITY_CHECK
    )

    end_execution = DummyOperator(task_id="end_execution")

    begin_execution >> create_tables

    create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table

    load_songplays_fact_table >> load_user_dimension_table
    load_songplays_fact_table >> load_song_dimension_table
    load_songplays_fact_table >> load_artist_dimension_table
    load_songplays_fact_table >> load_time_dimension_table

    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks

    run_quality_checks >> end_execution


FINAL_PROJECT_DAG = final_project()
