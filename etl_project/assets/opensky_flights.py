import pandas as pd
from etl_project.connectors.opensky_flights import OpenSkyApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta
import logging
import numpy as np


def _generate_hourly_datetime_ranges(
    start_datetime: str,
    end_datetime: str,
) -> list[dict[str, datetime]]:
    """
    Generates a range of hourly datetime ranges.

    Usage example:
        _generate_hourly_datetime_ranges(start_datetime="2025-01-01 00:00", end_datetime="2025-01-01 17:00")

    Returns:
            [
                {'start_time': datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)},
                {'start_time': datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2025, 1, 1, 2, 0, 0, tzinfo=timezone.utc)},
                ...
                {'start_time': datetime(2025, 1, 1, 16, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2025, 1, 1, 17, 0, 0, tzinfo=timezone.utc)}
            ]

    Args:
        start_datetime: provide a str with the format "yyyy-mm-dd HH:MM"
        end_datetime: provide a str with the format "yyyy-mm-dd HH:MM"

    Returns:
        A list of dictionaries with datetime objects

    Raises:
        Exception when incorrect input datetime string format is provided.
    """

    date_range = []
    if start_datetime is not None and end_datetime is not None:
        start_time = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )
        end_time = datetime.strptime(end_datetime, "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )
        total_hours = int((end_time - start_time).total_seconds() / 3600)
        date_range = [
            {
                "start_time": (start_time + timedelta(hours=i)),
                "end_time": (start_time + timedelta(hours=i + 1)),
            }
            for i in range(total_hours)
        ]
    else:
        raise Exception(
            "Please provide valid datetimes `YYYY-MM-DD HH:MM` for start_datetime and end_datetime."
        )
    return date_range


def extract_opensky_flights(
    opensky_client: OpenSkyApiClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Perform extraction using OpenSky API.
    """
    data = []
    for dates in _generate_hourly_datetime_ranges(
        start_date=start_date, end_date=end_date
    ):
        data.extend(
            opensky_client.get_flights(
                start_time=int(dates.get("start_time").timestamp()),
                end_time=int(dates.get("end_time").timestamp()),
            )
        )
    df = pd.json_normalize(data=data)
    return df


def transform_flight_data(df_flights: pd.DataFrame):
    """Performs transformation on dataframe produced from extract() function."""
    df_flights["firstSeen"] = pd.to_datetime(df_flights["firstSeen"], unit="s")
    df_flights["lastSeen"] = pd.to_datetime(df_flights["lastSeen"], unit="s")
    df_flights["estDepartureAirportDistance"] = np.sqrt(
        df_flights["estDepartureAirportHorizDistance"] ** 2
        + df_flights["estDepartureAirportVertDistance"] ** 2
    )
    df_flights["estArrivalAirportDistance"] = np.sqrt(
        df_flights["estArrivalAirportHorizDistance"] ** 2
        + df_flights["estArrivalAirportVertDistance"] ** 2
    )
    df_flights["estDepartureAirportDistance"] = (
        df_flights["estDepartureAirportDistance"].round(2).astype(float)
    )
    df_flights["estArrivalAirportDistance"] = (
        df_flights["estArrivalAirportDistance"].round(2).astype(float)
    )
    df_flights_transformed = df_flights[
        [
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
            "callsign",
            "estDepartureAirportDistance",
            "estArrivalAirportDistance",
        ]
    ]
    return df_flights_transformed


def enrich_airport_data(df_flights_transformed, df_airports):
    merged_departure = df_flights_transformed.merge(
        df_airports, left_on="estDepartureAirport", right_on="ident", how="left"
    ).drop("ident", axis=1)
    merged_departure = merged_departure.rename(
        columns={
            "type": "departure_airport_type",
            "name": "departure_airport_name",
            "elevation_ft": "departure_elevation_ft",
            "continent": "departure_continent",
            "iso_country": "departure_country",
            "iso_region": "departure_iso_region",
            "municipality": "departure_municipality",
            "icao_code": "departure_icao_code",
            "iata_code": "departure_iata_code",
            "gps_code": "departure_gps_code",
            "local_code": "departure_local_code",
            "coordinates": "departure_coordinates",
        }
    )

    final_merged = merged_departure.merge(
        df_airports, left_on="estArrivalAirport", right_on="ident", how="left"
    ).drop("ident", axis=1)
    final_merged = final_merged.rename(
        columns={
            "type": "arrival_airport_type",
            "name": "arrival_airport_name",
            "elevation_ft": "arrival_elevation_ft",
            "continent": "arrival_continent",
            "iso_country": "arrival_country",
            "iso_region": "arrival_iso_region",
            "municipality": "arrival_municipality",
            "icao_code": "arrival_icao_code",
            "iata_code": "arrival_iata_code",
            "gps_code": "arrival_gps_code",
            "local_code": "arrival_local_code",
            "coordinates": "arrival_coordinates",
        }
    )
    columns_to_keep = [
        "icao24",
        "firstSeen",
        "lastSeen",
        "estDepartureAirport",
        "estArrivalAirport",
        "callsign",
        "estDepartureAirportDistance",
        "estArrivalAirportDistance",
        "departure_airport_type",
        "departure_airport_name",
        "departure_country",
        "departure_coordinates",
        "arrival_airport_type",
        "arrival_airport_name",
        "arrival_country",
        "arrival_coordinates",
    ]
    df_filtered = final_merged[columns_to_keep]
    df_filtered["firstSeen"] = df_filtered["firstSeen"].astype("datetime64[ns]")
    df_filtered["lastSeen"] = df_filtered["lastSeen"].astype("datetime64[ns]")

    return df_filtered


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
    data = df.to_dict(orient="records")
    logging.info(f"Loading data with method: {load_method}")
    logging.info(f"Data: {data}")
    logging.info(f"Table: {table}")
    logging.info(f"Metadata: {metadata}")

    if load_method == "insert":
        postgresql_client.insert(data=data, table=table, metadata=metadata)
    elif load_method == "upsert":
        postgresql_client.upsert(data=data, table=table, metadata=metadata)
    elif load_method == "overwrite":
        postgresql_client.overwrite(data=data, table=table, metadata=metadata)
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
