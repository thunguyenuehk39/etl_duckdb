"DAG that runs a transformation on data in DuckDB using the Astro SDK."

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import pandas as pd
from typing import Dict
import datetime as dt

# import tools from the Astro SKD
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.global_variables import constants as c

# -------- #
# Datasets #
# -------- #

market_depth_dataset = Table(
    name=c.MARKET_DEPTH_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
)

# ----------------- #
# Astro SDK Queries #
# ----------------- #

# Create a reporting table
@aql.transform(pool="duckdb")
def create_market_depth_reporting_table(in_table: Table, date: dt.datetime):
    return """
        SELECT
            date,
            market,
            ticker,
            close,
            open,
            high,
            low,
            volume,
            amount
        FROM {{ in_table }}
        WHERE date = {{ date }}
        """

@aql.dataframe(pool="duckdb")
def get_ohlcv_market(in_table: Table, date: dt.datetime):
    # print ingested data to the logs
    gv.task_log.info(in_table)

    output_df = in_table

    ### YOUR CODE HERE ###

    # print result table to the logs
    gv.task_log.info(output_df)

    return output_df

# --- #
# DAG #
# --- #

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the market depth data is ready in DuckDB
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves market data and saves it to a local JSON.",
    tags=["stock", "cafef"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def transform_market_depth():

    create_market_depth_reporting_table(
        in_table=Table(name=c.MARKET_DEPTH_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
        date=dt.date.fromisoformat('2022-04-13'),
        output_table=Table(name=c.REPORT_MARKET_DEPTH_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
    )

transform_market_depth()
