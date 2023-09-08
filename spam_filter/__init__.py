from dagster import (
    Definitions,
    FilesystemIOManager,
    ScheduleDefinition,
    load_assets_from_modules,
)

from . import assets, database
from .assets import create_spam_model_job
from .resources import Database, ModelStorage

database.create_database()

all_assets = load_assets_from_modules([assets])

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

create_spam_model_schedule = ScheduleDefinition(
    job=create_spam_model_job,
    cron_schedule="0 0 1 * *",  # every month
)

defs = Definitions(
    assets=all_assets,
    schedules=[create_spam_model_schedule],
    resources={
        "io_manager": io_manager,
        "model_storage": ModelStorage(dir="./weights"),
        "database": Database(path="./database.duckdb"),
    },
)
