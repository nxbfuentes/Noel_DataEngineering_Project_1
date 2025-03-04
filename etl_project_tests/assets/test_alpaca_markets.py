import os
from pathlib import Path
from etl_project.assets.alpaca_markets import (
    _generate_datetime_ranges,
    extract_alpaca_markets,
    extract_exchange_codes,
    transform,
    load,
)
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
import pytest
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone


@pytest.fixture
def setup_extract():
    load_dotenv()


def test_generate_datetime_ranges():
    expected_results = [
        {
            "start_time": datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "end_time": datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        },
        {
            "start_time": datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            "end_time": datetime(2020, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
        },
    ]
    results = _generate_datetime_ranges(start_date="2020-01-01", end_date="2020-01-03")
    assert results == expected_results


def test_extract_alpaca_markets(setup_extract):
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    alpaca_markets_client = AlpacaMarketsApiClient(
        api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY
    )
    df = extract_alpaca_markets(
        alpaca_markets_client, "TSLA", "2023-02-06", "2023-02-8"
    )
    assert len(df) == 2000


@pytest.fixture
def setup_input_alpaca_df():
    return pd.DataFrame(
        [
            {"i": 1, "t": 1, "x": "A", "p": 10, "s": 100},
            {"i": 2, "t": 2, "x": "B", "p": 20, "s": 200},
            {"i": 3, "t": 3, "x": "C", "p": 30, "s": 300},
        ]
    )


@pytest.fixture
def setup_exchange_codes_df():
    return extract_exchange_codes(
        "etl_project_tests/data/alpaca_markets/exchange_codes.csv"
    )


@pytest.fixture
def setup_transformed_alpaca_df():
    return pd.DataFrame(
        [
            {
                "id": 1,
                "timestamp": 1,
                "price": 10,
                "size": 100,
                "exchange": "NYSE American (AMEX)",
            },
            {
                "id": 2,
                "timestamp": 2,
                "price": 20,
                "size": 200,
                "exchange": "NASDAQ OMX BX",
            },
            {
                "id": 3,
                "timestamp": 3,
                "price": 30,
                "size": 300,
                "exchange": "National Stock Exchange",
            },
        ]
    )


def test_transform(
    setup_input_alpaca_df, setup_exchange_codes_df, setup_transformed_alpaca_df
):
    alpaca_df = setup_input_alpaca_df
    df_exchange_codes = setup_exchange_codes_df
    expected_df = setup_transformed_alpaca_df
    df = transform(df=alpaca_df, df_exchange_codes=df_exchange_codes)
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)


@pytest.fixture
def setup_transformed_dataframe():
    return pd.DataFrame(
        [
            {"id": 1, "name": "bob", "value": 100},
            {"id": 2, "name": "sam", "value": 200},
            {"id": 3, "name": "sarah", "value": 300},
        ]
    )


@pytest.fixture
def setup_postgresql_client():
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    return postgresql_client


@pytest.fixture
def setup_transformed_table_metadata():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("value", Integer),
    )
    return table_name, table, metadata


def test_load(
    setup_postgresql_client,
    setup_transformed_dataframe,
    setup_transformed_table_metadata,
):
    postgresql_client = setup_postgresql_client
    df = setup_transformed_dataframe
    table_name, table, metadata = setup_transformed_table_metadata
    postgresql_client.drop_table(table_name)  # reset
    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    assert len(postgresql_client.select_all(table=table)) == 3

    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    assert len(postgresql_client.select_all(table=table)) == 3
    postgresql_client.drop_table(table_name)  # reset
