"""SQL client handling.

This includes MSSQLStream and MSSQLConnector.
"""

from __future__ import annotations

import typing as t

import pyodbc
import sqlalchemy as sa
from singer_sdk import SQLConnector, SQLStream

from tap_mssql.json_serializer import deserialize_json, serialize_json


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def __init__(self, config: dict, sqlalchemy_url: str | None = None) -> None:
        """Initialize the SQL connector.

        Args:
            config: Connection configuration dictionary.
            sqlalchemy_url: Optional SQLAlchemy URL string.
        """
        if config.get("driver_type") == "pyodbc":
            pyodbc.pooling = False

        self.serialize_json = serialize_json
        self.deserialize_json = deserialize_json

        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(self, config: dict[str, t.Any]) -> str:
        """Return the SQLAlchemy URL string.

        Args:
          config: A dictionary of settings from the tap or target config.

        Returns:
          The URL as a string.
        """
        url_driver = f"{config.get("dialect")}+{config.get('driver_type')}"

        config_url = sa.URL.create(
            url_driver,
            config.get("user"),
            config.get("password"),
            host=config.get("host"),
            database=config.get("database"),
            port=config.get("port", 1433),
        )

        if "sqlalchemy_url_query" in config:
            config_url = config_url.update_query_dict(config.get("sqlalchemy_url_query"))  # type: ignore  # noqa: PGH003

        return str(config_url)


class MSSQLStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = MSSQLConnector

    def get_records(self, partition: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
