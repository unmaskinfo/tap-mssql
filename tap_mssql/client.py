"""SQL client handling.

This includes MSSQLStream and MSSQLConnector.
"""

from __future__ import annotations

import datetime
import typing as t
from base64 import b64encode

import pyodbc
import sqlalchemy as sa
from singer_sdk import SQLConnector, SQLStream

from tap_mssql.json_serializer import deserialize_json, serialize_json

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from sqlalchemy.engine import Engine
    from sqlalchemy.sql.type_api import TypeEngine


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def __init__(self, config: dict, sqlalchemy_url: str | None = None) -> None:
        """Initialize the SQL connector.

        Args:
            config: Connection configuration dictionary.
            sqlalchemy_url: Optional SQLAlchemy URL string.
        """
        self.mssql_connection_config = config.get("mssql_connection_config", {})
        if self.mssql_connection_config.get("driver_type") == "pyodbc":
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
        url_driver = f"mssql+{self.mssql_connection_config.get('driver_type')}"

        config_url = sa.URL.create(
            url_driver,
            self.mssql_connection_config.get("user"),
            self.mssql_connection_config.get("password"),
            host=self.mssql_connection_config.get("host"),
            database=self.mssql_connection_config.get("database"),
            port=self.mssql_connection_config.get("port", 1433),
        )

        if "sqlalchemy_url_query" in self.mssql_connection_config:
            config_url = config_url.update_query_dict(self.mssql_connection_config.get("sqlalchemy_url_query"))  # type: ignore  # noqa: PGH003

        return config_url

    def create_engine(self) -> Engine:
        """Create a new SQLAlchemy engine instance.

        Returns:
            A new SQLAlchemy engine instance.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": False,
            f"{eng_prefix}json_serializer": self.serialize_json,
            f"{eng_prefix}json_deserializer": self.deserialize_json,
            **{
                f"{eng_prefix}{key}": value
                for key, value in self.mssql_connection_config.get("sqlalchemy_eng_params", {}).items()
            },
        }
        self.logger.info(f"SQLAlchemy URL: {eng_config}")
        return sa.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(self, sql_type: str | TypeEngine | type[TypeEngine] | t.Any) -> dict:  # noqa: ANN401
        """Convert SQL type to JSONSchema type.

        Args:
            sql_type: SQL type to convert

        Returns:
            JSONSchema type definition
        """
        error_msg = "hd_jsonschema_types is implemented in the tap_mssql package"
        if self.config.get("hd_jsonschema_types"):
            raise NotImplementedError(error_msg)

        return self.org_to_jsonschema_type(sql_type)

    @classmethod
    def org_to_jsonschema_type(cls, sql_type: str | TypeEngine | type[TypeEngine] | object) -> dict:
        """Convert SQL type to JSONSchema type.

        Args:
            sql_type: SQL type to convert

        Returns:
            JSONSchema type definition
        """
        if str(sql_type).startswith("NUMERIC"):
            sql_type = "int" if str(sql_type).endswith(", 0)") else "number"

        if str(sql_type) in ["MONEY", "SMALLMONEY"]:
            sql_type = "number"

        if str(sql_type) in ["BIT"]:
            sql_type = bool

        if str(sql_type) in ["ROWVERSION", "TIMESTAMP"]:
            sql_type = "string"

        return super(MSSQLConnector, cls).to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: str) -> sa.types.TypeEngine:
        """Convert JSONSchema type to SQL type.

        Args:
            jsonschema_type: JSONSchema type to convert

        Returns:
            SQLAlchemy type engine instance
        """
        return SQLConnector.to_sql_type(jsonschema_type)  # type: ignore  # noqa: PGH003


class MSSQLStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = MSSQLConnector
    supports_nulls_first: bool = False

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Process the record after it has been extracted from the database.

        Args:
            row: Individual record from database
            context: Stream partition or context dictionary

        Returns:
            The processed record
        """
        record: dict = row
        properties: dict = self.schema.get("properties", {})

        for key, value in record.items():
            if value is not None:
                property_schema: dict = properties.get(key, {})

                # Date in ISO format
                if isinstance(value, datetime.datetime):
                    record.update({key: value.isoformat()})

                # Encode base64 binary fields in the record
                if property_schema.get("contentEncoding") == "base64" and isinstance(value, bytes):
                    record.update({key: b64encode(value).decode("utf-8")})

        return record

    def get_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            order_by = (
                sa.nulls_first(replication_key_col.asc()) if self.supports_nulls_first else replication_key_col.asc()
            )
            query = query.order_by(order_by)

            if replication_key_col.type.python_type in (datetime.datetime, datetime.date):
                start_val = self.get_starting_timestamp(context)
            else:
                start_val = self.get_starting_replication_key_value(context)

            if start_val:
                query = query.where(replication_key_col >= start_val)

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    continue
                yield transformed_record
