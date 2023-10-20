"DAG that runs a transformation on data in DuckDB using the Astro SDK."

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
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
from include.market_depth import (
    cafef_market_depth
)

# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("start")

# ----------------- #
# Astro SDK Queries #
# ----------------- #

@aql.dataframe(pool="duckdb")
def turn_json_into_table(in_json):
    """Converts the list of JSON input into one pandas dataframe."""
    if type(in_json) == dict:
        df = pd.DataFrame(in_json)
    else:
        df = pd.concat([pd.DataFrame(i) for i in in_json], ignore_index=True)
    return df

# Create a reporting table
# @aql.transform(pool="duckdb")
# def create_market_depth_reporting_table(in_table: Table, date: dt.datetime):
#     return """
#         SELECT
#             date,
#             market,
#             ticker,
#             close,
#             open,
#             high,
#             low,
#             volume,
#             amount
#         FROM {{ in_table }}
#         WHERE date = {{ date }}
#         """

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the market depth data is ready in DuckDB
    schedule=[start_dataset],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves market data and saves it to a local JSON.",
    tags=["stock", "cafef"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_market_depth_indate():
    @task
    def get_market_depth(market, date):
        """Use the 'get_market_depth' function from the local
        'market_depth' module to retrieve the market depth."""
        market_depth_data = cafef_market_depth(market=market, date=date)
        if market_depth_data[1] == {}:
            return market_depth_data[0].to_dict(orient="records")
        else:
            market_depth_data[1]
    date_time = dt.date.fromisoformat('2022-04-13')
    data_market = get_market_depth(market=uv.MARKET, date=date_time)

    turn_json_into_table(
        in_json=data_market,
        output_table=Table(name=c.MARKET_DEPTH_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
    )

extract_market_depth_indate()
