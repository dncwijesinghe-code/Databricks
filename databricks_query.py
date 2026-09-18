"""Run a read-only SQL query against the configured Databricks warehouse."""

import os
import sys

from dotenv import load_dotenv
from databricks import sql


def required_setting(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required setting: {name}")
    return value


def main():
    load_dotenv()
    query = " ".join(sys.argv[1:]).strip() or "SELECT current_user(), current_catalog()"

    host = required_setting("DATABRICKS_HOST").rstrip("/")
    if host.startswith("https://"):
        host = host[len("https://"):]
    http_path = required_setting("DATABRICKS_HTTP_PATH")
    token = required_setting("DATABRICKS_TOKEN")

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description or []]
            print("\t".join(columns))
            for row in cursor.fetchall():
                print("\t".join("" if value is None else str(value) for value in row))


if __name__ == "__main__":
    main()
