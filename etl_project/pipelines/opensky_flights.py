from dotenv import load_dotenv
import os
from etl_project.connectors.opensky_flights import OpenSkyApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime
from etl_project.assets.opensky_flights import (
    extract_opensky_flights,
    transform_flight_data,
    enrich_airport_data,
    load,
)
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
import yaml
from pathlib import Path
import schedule
import time
from etl_project.assets.pipeline_logging import PipelineLogging
import pandas as pd


def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    if not all([SERVER_NAME, DATABASE_NAME, DB_USERNAME, DB_PASSWORD, PORT]):
        pipeline_logging.logger.error("Missing one or more environment variables")
        raise EnvironmentError("Missing one or more environment variables")

    pipeline_logging.logger.info("Creating OpenSky API client")
    opensky_client = OpenSkyApiClient()

    # extract
    pipeline_logging.logger.info("Extracting data from OpenSky API")
    df_opensky_flights = extract_opensky_flights(
        opensky_client=opensky_client,
        start_time=config.get("start_time"),
        end_time=config.get("end_time"),
    )
    pipeline_logging.logger.debug(f"Extracted data: {df_opensky_flights.head()}")

    # transform
    pipeline_logging.logger.info("Transforming dataframes")
    df_transformed = transform_flight_data(response_data=df_opensky_flights)
    pipeline_logging.logger.debug(f"Transformed data: {df_transformed.head()}")

    df_airports = pd.read_csv(config.get("airport_codes_path"))
    pipeline_logging.logger.debug(f"Airport data: {df_airports.head()}")
    df_enriched = enrich_airport_data(
        df_flights_transformed=df_transformed, df_airports=df_airports
    )
    pipeline_logging.logger.debug(f"Enriched data: {df_enriched.head()}")

    # load
    pipeline_logging.logger.info("Loading data to postgres")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = Table(
        "opensky_flights",
        metadata,
        Column("icao24", String, primary_key=True),
        Column("firstSeen", DateTime),
        Column("lastSeen", DateTime),
        Column("estDepartureAirport", String),
        Column("estArrivalAirport", String),
        Column("callsign", String),
        Column("estDepartureAirportDistance", Float),
        Column("estArrivalAirportDistance", Float),
        Column("departure_airport_type", String),
        Column("departure_airport_name", String),
        Column("departure_country", String),
        Column("departure_coordinates", String),
        Column("arrival_airport_type", String),
        Column("arrival_airport_name", String),
        Column("arrival_country", String),
        Column("arrival_coordinates", String),
    )
    load(
        df=df_enriched,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    pipeline_logging.logger.info("Pipeline run successful")


def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_name,
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log(status=MetaDataLoggingStatus.RUNNING)  # log start
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()


if __name__ == "__main__":
    # set up environment variables
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")


    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")


    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config,
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
    except KeyboardInterrupt:
        print("Pipeline execution interrupted by user.")
