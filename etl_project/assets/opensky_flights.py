import pandas as pd
from etl_project.connectors.opensky_flights import OpenSkyApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta


def extract_opensky_flights(
    opensky_client: OpenSkyApiClient,
    start_time: int,
    end_time: int,
) -> pd.DataFrame:
    """
    Perform extraction using OpenSky API.
    """
    data = opensky_client.get_flights(start_time=start_time, end_time=end_time)
    df = pd.json_normalize(data=data)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Performs transformation on dataframe produced from extract() function."""
    df["firstSeen"] = pd.to_datetime(df["firstSeen"], unit="s")
    df["lastSeen"] = pd.to_datetime(df["lastSeen"], unit="s")
    df["estDepartureAirportDistance"] = (
        df["estDepartureAirportHorizDistance"] ** 2
        + df["estDepartureAirportVertDistance"] ** 2
    ) ** 0.5
    df["estArrivalAirportDistance"] = (
        df["estArrivalAirportHorizDistance"] ** 2
        + df["estArrivalAirportVertDistance"] ** 2
    ) ** 0.5
    return df


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
