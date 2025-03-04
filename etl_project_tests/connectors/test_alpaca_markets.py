from dotenv import load_dotenv
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
import os
import pytest
from datetime import datetime, timezone


@pytest.fixture
def setup():
    load_dotenv()


def test_weather_client_get_city_by_name(setup):
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    alpaca_markets_client = AlpacaMarketsApiClient(
        api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY
    )
    start_time = datetime(
        year=2023, month=2, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc
    ).isoformat()
    end_time = datetime(
        year=2023, month=2, day=1, hour=23, minute=59, second=59, tzinfo=timezone.utc
    ).isoformat()
    data = alpaca_markets_client.get_trades(
        "TSLA", start_time=start_time, end_time=end_time
    )

    assert type(data) == list
    assert len(data) == 1000
