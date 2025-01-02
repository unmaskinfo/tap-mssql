"""SQL client handling.

This includes MSSQLStream and MSSQLConnector.
"""

from __future__ import annotations

import typing as t

import sqlalchemy as sa
from singer_sdk import SQLConnector, SQLStream


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        mssql_config = config.get("mssql")
        driver = f"mssql+{mssql_config.get('driver_type')}"
        return sa.URL.create(
            drivername=driver,
            username=mssql_config.get("user"),
            password=mssql_config.get("password"),
            host=mssql_config.get("host"),
            port=mssql_config.get("port"),
            database=mssql_config.get("database"),
        )


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
