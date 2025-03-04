from enum import Enum
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func


class MetaDataLoggingStatus(Enum):
    RUN_SUCCESS = "SUCCESS"
    RUN_FAILURE = "FAILURE"


class MetaDataLogging:
    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
        config: dict = {},
        log_table_name: str = "opensky_pipeline_logs",
    ):
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()
        self.table = Table(
            self.log_table_name,
            self.metadata,
            Column("pipeline_name", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("timestamp", String, primary_key=True),
            Column("status", String, primary_key=True),
            Column("config", JSON),
            Column("logs", String),
        )
        self.run_id = self._get_run_id()

    def _create_log_table(self) -> None:
        """Create log table if it does not exist."""
        self.postgresql_client.create_table(metadata=self.metadata)

    def _get_run_id(self):
        """Gets the next run id. Sets run id to 1 if no run id exists."""
        self._create_log_table()
        run_id = self.postgresql_client.engine.execute(
            select(func.max(self.table.c.run_id)).where(
                self.table.c.pipeline_name == self.pipeline_name
            )
        ).first()[0]
        if run_id is None:
            return 1
        else:
            return run_id + 1

    def log(self, status: MetaDataLoggingStatus = None, logs: str = None):
        log_entry = {
            "pipeline_name": self.pipeline_name,
            "status": status.value if status else None,
            "logs": logs,
            "timestamp": datetime.now().isoformat(),
        }
        insert_statement = insert(self.table).values(
            pipeline_name=self.pipeline_name,
            timestamp=log_entry["timestamp"],
            run_id=self.run_id,
            status=log_entry["status"],
            config=self.config,
            logs=log_entry["logs"],
        )
        self.postgresql_client.engine.execute(insert_statement)
