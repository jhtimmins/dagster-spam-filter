import json
import os

import duckdb
import pandas as pd
from dagster import ConfigurableResource
from duckdb import DuckDBPyConnection
from pydantic import PrivateAttr


class Database(ConfigurableResource):
    path: str
    _conn: DuckDBPyConnection = PrivateAttr()

    def __enter__(self):
        self._conn = duckdb.connect(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()

    def execute(self, body):
        if self._conn is None:
            raise Exception(
                "Database connection not initialized. Use the object within a 'with' block."
            )

        self._conn.execute(body)

    def query(self, body: str):
        with duckdb.connect(self.path) as conn:
            result = conn.query(body)
            if result is None:
                return pd.DataFrame()
            else:
                return result.to_df()


class ModelStorage(ConfigurableResource):
    dir: str

    def setup_for_execution(self, context) -> None:
        os.makedirs(self.dir, exist_ok=True)

    def write(self, filename, data):
        with open(f"{self.dir}/{filename}", "w") as f:
            json.dump(data, f)

    def read(self, filename):
        with open(f"{self.dir}/{filename}", "r") as f:
            return json.load(f)
