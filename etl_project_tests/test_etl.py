import unittest
import pandas as pd
from etl_project.etl import (
    fetch_flight_data,
    transform_flight_data,
    enrich_airport_data,
)


class TestETL(unittest.TestCase):

    def test_transform_flight_data(self):
        response_data = [
            {
                "icao24": "a9519b",
                "firstSeen": 1735661721,
                "estDepartureAirport": "KFWS",
                "lastSeen": 1735662419,
                "estArrivalAirport": "75TE",
                "callsign": "DCM2700",
                "estDepartureAirportHorizDistance": 3457.0,
                "estDepartureAirportVertDistance": 487.0,
                "estArrivalAirportHorizDistance": 8714.0,
                "estArrivalAirportVertDistance": 1455.0,
            }
        ]
        df_flights_transformed = transform_flight_data(response_data)
        self.assertEqual(df_flights_transformed.shape[1], 8)
        self.assertIn("icao24", df_flights_transformed.columns)
        self.assertIn("firstSeen", df_flights_transformed.columns)

    def test_enrich_airport_data(self):
        df_flights_transformed = pd.DataFrame(
            {
                "icao24": ["a9519b"],
                "firstSeen": [pd.to_datetime(1735661721, unit="s")],
                "estDepartureAirport": ["KFWS"],
                "lastSeen": [pd.to_datetime(1735662419, unit="s")],
                "estArrivalAirport": ["75TE"],
                "callsign": ["DCM2700"],
                "estDepartureAirportDistance": [3491.13],
                "estArrivalAirportDistance": [8834.64],
            }
        )
        df_airports = pd.DataFrame(
            {
                "ident": ["KFWS", "75TE"],
                "type": ["small_airport", "small_airport"],
                "name": ["Fort Worth Spinks Airport", "Arrival Airport"],
                "elevation_ft": [700, 800],
                "continent": ["NA", "NA"],
                "iso_country": ["US", "US"],
                "iso_region": ["US-TX", "US-TX"],
                "municipality": ["Fort Worth", "Arrival City"],
                "icao_code": ["KFWS", "75TE"],
                "iata_code": ["FWS", "TEA"],
                "gps_code": ["KFWS", "75TE"],
                "local_code": ["FWS", "TEA"],
                "coordinates": ["32.5656, -97.3086", "31.7474, -97.2461"],
            }
        )
        final_merged = enrich_airport_data(df_flights_transformed, df_airports)
        self.assertIn("departure_airport_name", final_merged.columns)
        self.assertIn("arrival_airport_name", final_merged.columns)
        self.assertEqual(final_merged.shape[1], 20)


if __name__ == "__main__":
    unittest.main()
