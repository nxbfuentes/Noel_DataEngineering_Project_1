import os
from pathlib import Path
from etl_project.assets.weather import extract_weather, extract_population, transform
from etl_project.connectors.weather_api import WeatherApiClient
import pytest
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.weather import load
from datetime import datetime


@pytest.fixture
def setup_extract():
    load_dotenv()
    return os.environ.get("API_KEY")


def test_extract_weather(setup_extract):
    API_KEY = setup_extract
    weather_api_client = WeatherApiClient(api_key=API_KEY)
    df = extract_weather(
        weather_api_client=weather_api_client,
        city_reference_path=Path(
            "./etl_project_tests/data/weather/australian_capital_cities.csv"
        ),
    )
    assert len(df) == 8


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
def setup_input_df_weather():
    return pd.DataFrame(
        [
            {
                "name": "perth",
                "dt": datetime(2020, 1, 1, 0, 0, 0),
                "id": 1,
                "main.temp": 20,
            },
            {
                "name": "sydney",
                "dt": datetime(2020, 1, 1, 0, 0, 0),
                "id": 2,
                "main.temp": 22,
            },
        ]
    )


@pytest.fixture
def setup_input_df_population():
    return extract_population(
        "etl_project_tests/data/weather/australian_city_population.csv"
    )


@pytest.fixture
def setup_transformed_df():
    return pd.DataFrame(
        [
            {
                "datetime": datetime(2020, 1, 1, 0, 0, 0),
                "id": 1,
                "name": "perth",
                "temperature": 20,
                "population": 2141834,
                "unique_id": "2020-01-011",
            },
            {
                "datetime": datetime(2020, 1, 1, 0, 0, 0),
                "id": 2,
                "name": "sydney",
                "temperature": 22,
                "population": 5361466,
                "unique_id": "2020-01-012",
            },
        ]
    ).set_index(["unique_id"])


def test_transform(
    setup_input_df_weather, setup_input_df_population, setup_transformed_df
):
    df_weather = setup_input_df_weather
    df_population = setup_input_df_population
    expected_df = setup_transformed_df
    df = transform(df_weather=df_weather, df_population=df_population)
    print(df)
    print(expected_df)
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
