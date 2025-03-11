from dotenv import load_dotenv
from etl_project.connectors.weather_api import WeatherApiClient
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_weather_client_get_city_by_name(setup):
    API_KEY = os.environ.get("API_KEY")
    weather_api_client = WeatherApiClient(api_key=API_KEY)
    data = weather_api_client.get_city(city_name="Perth", temperature_units="metric")

    assert type(data) == dict
    assert len(data) > 0
