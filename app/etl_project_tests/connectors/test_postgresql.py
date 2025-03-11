from etl_project.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData


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
def setup_table():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", String),
    )
    return table_name, table, metadata


def test_postgresql_connection(setup_postgresql_client):
    """
    Test the connection to the PostgreSQL database.
    """
    postgresql_client = setup_postgresql_client
    try:
        connection = postgresql_client.engine.connect()
        connection.close()
        assert True
    except Exception as e:
        assert False, f"Connection failed: {e}"


def test_insert_data_into_postgresql_table(setup_postgresql_client, setup_table):
    """
    Test inserting data into a PostgreSQL table.
    """
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(
        table_name
    )  # Ensure the table is dropped before the test

    data = [
        {"id": 1, "value": "expected_value_1"},
        {"id": 2, "value": "expected_value_2"},
    ]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)


def test_upsert_data_into_postgresql_table(setup_postgresql_client, setup_table):
    """
    Test upserting data into a PostgreSQL table.
    """
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(
        table_name
    )  # Ensure the table is dropped before the test

    data = [
        {"id": 1, "value": "expected_value_1"},
        {"id": 2, "value": "expected_value_2"},
    ]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.upsert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)


def test_overwrite_data_in_postgresql_table(setup_postgresql_client, setup_table):
    """
    Test overwriting data in a PostgreSQL table.
    """
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(
        table_name
    )  # Ensure the table is dropped before the test

    data = [
        {"id": 1, "value": "expected_value_1"},
        {"id": 2, "value": "expected_value_2"},
    ]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.overwrite(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)
