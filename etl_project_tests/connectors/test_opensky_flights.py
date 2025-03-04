from dotenv import load_dotenv
from etl_project.connectors.opensky_flights import OpenSkyApiClient
import os
import pytest
from datetime import datetime, timezone


@pytest.fixture
def setup():
    load_dotenv()


def test_opensky_client_get_flights(setup):
    opensky_client = OpenSkyApiClient()
    start_time = int(
        datetime(
            year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc
        ).timestamp()
    )
    end_time = int(
        datetime(
            year=2025,
            month=1,
            day=1,
            hour=1,
            minute=0,
            second=0,
            tzinfo=timezone.utc,
        ).timestamp()
    )
    data = opensky_client.get_flights(start_time=start_time, end_time=end_time)

    assert type(data) == list
    assert len(data) > 0
