import pandas as pd
from etl_project.connectors.opensky_flights import OpenSkyApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta
import logging


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


def transform_flight_data(response_data):
    df_flights = pd.json_normalize(data=response_data)
    df_flights["firstSeen"] = pd.to_datetime(df_flights["firstSeen"], unit="s")
    df_flights["lastSeen"] = pd.to_datetime(df_flights["lastSeen"], unit="s")
    df_flights["estDepartureAirportDistance"] = (
        df_flights["estDepartureAirportHorizDistance"] ** 2
        + df_flights["estDepartureAirportVertDistance"] ** 2
    ) ** 0.5
    df_flights["estArrivalAirportDistance"] = (
        df_flights["estArrivalAirportHorizDistance"] ** 2
        + df_flights["estArrivalAirportVertDistance"] ** 2
    ) ** 0.5
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
