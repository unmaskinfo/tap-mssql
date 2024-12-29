"""MSSQL tap class.

- Meltano SDK Config Schema: https://sdk.meltano.com/en/v0.43.1/guides/config-schema.html
- Build SQL Taps: https://sdk.meltano.com/en/v0.43.1/guides/sql-tap.html
"""

from __future__ import annotations

import sys

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_mssql.client import MSSQLStream


class TapMSSQL(SQLTap):
    """MSSQL tap class."""

    name = "tap-mssql"
    default_stream_class = MSSQLStream
    default_output = sys.stdout.buffer

    config_jsonschema = th.PropertiesList(
        th.Property(
            "mssql_connection_config",
            th.ObjectType(
                th.Property(
                    "host",
                    th.StringType,
                    description="The hostname or IP address of the MSSQL server",
                    required=True,
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    description="The port number for the MSSQL server",
                    default=1433,
                ),
                th.Property(
                    "database",
                    th.StringType,
                    description="The name of the database to connect to",
                    required=True,
                ),
                th.Property(
                    "user",
                    th.StringType,
                    description="Username used to authenticate with the MSSQL server",
                    required=True,
                ),
                th.Property(
                    "password",
                    th.StringType,
                    description="Password used to authenticate with the MSSQL server",
                    required=True,
                    secret=True,  # Marks this field as sensitive
                ),
                th.Property(
                    "driver_type",
                    th.StringType,
                    description="The Python database driver to use for connecting to MSSQL Server.",
                    allowed_values=["pyodbc", "pymssql"],
                    default="pyodbc",
                    required=True,
                ),
            ),
            description="MSSQL connection configuration",
            required=True,
        )
    ).to_dict()


if __name__ == "__main__":
    TapMSSQL.cli()
