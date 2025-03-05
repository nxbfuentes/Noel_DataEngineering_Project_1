import unittest
from unittest.mock import patch, MagicMock
from etl_project.pipelines.opensky_flights import pipeline, run_pipeline
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus


class TestOpenSkyFlightsPipeline(unittest.TestCase):

    @patch("etl_project.pipelines.opensky_flights.extract_opensky_flights")
    @patch("etl_project.pipelines.opensky_flights.transform_flight_data")
    @patch("etl_project.pipelines.opensky_flights.enrich_airport_data")
    @patch("etl_project.pipelines.opensky_flights.load")
    @patch("etl_project.pipelines.opensky_flights.OpenSkyApiClient")
    @patch("etl_project.pipelines.opensky_flights.PostgreSqlClient")
    def test_pipeline(
        self,
        MockPostgreSqlClient,
        MockOpenSkyApiClient,
        mock_load,
        mock_enrich_airport_data,
        mock_transform_flight_data,
        mock_extract_opensky_flights,
    ):
        # Mock the methods and classes
        mock_opensky_client = MockOpenSkyApiClient.return_value
        mock_postgresql_client = MockPostgreSqlClient.return_value
        mock_extract_opensky_flights.return_value = MagicMock()
        mock_transform_flight_data.return_value = MagicMock()
        mock_enrich_airport_data.return_value = MagicMock()

        config = {
            "start_time": "2023-01-01T00:00:00Z",
            "end_time": "2023-01-01T01:00:00Z",
            "airport_codes_path": "path/to/airport_codes.csv",
        }
        pipeline_logging = PipelineLogging(
            pipeline_name="test_pipeline", log_folder_path="path/to/logs"
        )

        # Run the pipeline
        pipeline(config=config, pipeline_logging=pipeline_logging)

        # Assert the methods were called
        mock_extract_opensky_flights.assert_called_once_with(
            opensky_client=mock_opensky_client,
            start_time=config["start_time"],
            end_time=config["end_time"],
        )
        mock_transform_flight_data.assert_called_once()
        mock_enrich_airport_data.assert_called_once()
        mock_load.assert_called_once()

    @patch("etl_project.pipelines.opensky_flights.PipelineLogging")
    @patch("etl_project.pipelines.opensky_flights.MetaDataLogging")
    @patch("etl_project.pipelines.opensky_flights.pipeline")
    def test_run_pipeline(
        self, mock_pipeline, MockMetaDataLogging, MockPipelineLogging
    ):
        # Mock the methods and classes
        mock_metadata_logging = MockMetaDataLogging.return_value
        mock_pipeline_logging = MockPipelineLogging.return_value

        pipeline_config = {"config": {"log_folder_path": "path/to/logs"}}
        postgresql_logging_client = MagicMock()

        # Run the run_pipeline function
        run_pipeline(
            pipeline_name="test_pipeline",
            postgresql_logging_client=postgresql_logging_client,
            pipeline_config=pipeline_config,
        )

        # Assert the methods were called
        mock_metadata_logging.log.assert_called()
        mock_pipeline.assert_called_once_with(
            config=pipeline_config["config"], pipeline_logging=mock_pipeline_logging
        )
        mock_metadata_logging.log.assert_called_with(
            status=MetaDataLoggingStatus.RUN_SUCCESS,
            logs=mock_pipeline_logging.get_logs(),
        )


if __name__ == "__main__":
    unittest.main()
