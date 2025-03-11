import requests
import pandas as pd
import time
import numpy as np
from sqlalchemy import Table, MetaData
from connectors.postgresql import PostgreSqlClient


def fetch_flight_data(start_time, end_time):
    root_url = "https://opensky-network.org/api/flights/all"
    params = {"begin": start_time, "end": end_time}
    response = requests.get(f"{root_url}", params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def transform_flight_data(response_data):
    df_flights = pd.json_normalize(data=response_data)
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
            "type": "departure_type",
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
            "type": "arrival_type",
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
    return final_merged


def load(df, postgresql_client, table, metadata, load_method="overwrite"):
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


def main():
    current_time = int(
        time.mktime(time.strptime("2025-01-01 01:00:00", "%Y-%m-%d %H:%M:%S"))
    )
    one_hour_ago = current_time - 3600

    response_data = fetch_flight_data(one_hour_ago, current_time)

    df_flights_transformed = transform_flight_data(response_data)

    df_airports = pd.read_csv("data/airport-codes.csv")
    final_merged = enrich_airport_data(df_flights_transformed, df_airports)

    postgresql_client = PostgreSqlClient()
    metadata = MetaData()
    table = Table("flights", metadata, autoload_with=postgresql_client.engine)
    load(final_merged, postgresql_client, table, metadata, load_method="overwrite")

    print(final_merged.head())
    print(final_merged.columns)


if __name__ == "__main__":
    main()
