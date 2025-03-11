# Code Citations

## License: unknown
https://github.com/snehakadam25/nba-data-engineering/tree/e48cee214a584fb36cad6a343f142bfb32a34d7a/etl_project/assets/nba.py

```
"
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data
```


## License: unknown
https://github.com/AmyChude/DataEngineeringWork/tree/0d324cacde256b352f5ba092cce7a71375630c7b/Pythonetl/3/13-evr-multi-pipelines/solved/etl_project/pipelines/alpaca_markets.py

```
)
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
import yaml
from pathlib import Path
import schedule
import time
from etl_project.assets.pipeline_logging import PipelineLogging

def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    # set
```


## License: unknown
https://github.com/AmyChude/DataEngineeringWork/tree/0d324cacde256b352f5ba092cce7a71375630c7b/Pythonetl/3/13-evr-multi-pipelines/unsolved/etl_project/pipelines/alpaca_markets.py

```
yaml
from pathlib import Path
import schedule
import time
from etl_project.assets.pipeline_logging import PipelineLogging

def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline
```


## License: unknown
https://github.com/AmyChude/DataEngineeringWork/tree/0d324cacde256b352f5ba092cce7a71375630c7b/Pythonetl/3/05-ins-logging/solved/etl_project_tests/connectors/test_postgresql.py

```
.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")
```

