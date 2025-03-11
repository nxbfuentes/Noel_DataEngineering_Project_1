from datetime import datetime, timezone
from etl_project.assets.opensky_flights import _generate_hourly_datetime_ranges


def test_generate_hourly_datetime_ranges():
    start_datetime = "2025-01-01 00:00"
    end_datetime = "2025-01-01 16:00"
    result = _generate_hourly_datetime_ranges(start_datetime, end_datetime)
    for range_dict in result:
        print(f"Start: {range_dict['start_time']}, End: {range_dict['end_time']}")


if __name__ == "__main__":
    test_generate_hourly_datetime_ranges()
