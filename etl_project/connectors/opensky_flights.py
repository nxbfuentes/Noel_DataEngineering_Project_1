import requests


class OpenSkyApiClient:
    def __init__(self):
        self.base_url = "https://opensky-network.org/api"

    def get_flights(self, start_time: int, end_time: int) -> list[dict]:
        """
        Get the flights data for a specific time range.

        Args:
            start_time: start time in epoch seconds
            end_time: end time in epoch seconds

        Returns:
            A list of flights between the start and end times

        Raises:
            Exception if response code is not 200.
        """
        url = f"{self.base_url}/flights/all"
        params = {"begin": start_time, "end": end_time}
        print(f"Request URL: {url}")
        print(f"Request Parameters: {params}")
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to extract data from OpenSky API. Status Code: {response.status_code}. Response: {response.text}"
            )
