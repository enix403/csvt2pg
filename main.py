from sqlalchemy import create_engine, Engine, text, DDL
from sqlalchemy.engine import URL
import click

from configparser import ConfigParser

config = ConfigParser()
with open("config.txt") as stream:
    # Append a fake section
    config.read_string("[top]\n" + stream.read())

config = config['top']

C_DB_NAME = config.get('db_name', None)
C_DB_USER = config.get('db_user', None)
C_DB_PASSWORD = config.get('db_password', None)
C_DB_HOST = config.get('db_host', None)
C_DB_PORT = config.get('db_port', None)
C_TABLE_NAME = config.get('table_name', None)
C_CSV_DIRECTORY = config.get('csv_directory', None)

SCHEMA_NAME = "public"

def parse_port(port: str):
    try:
        num = int(port)
        return num
    except:
        return 5432

def create_url(use_database: bool = True):
    return URL.create(
        drivername="postgresql",
        username=C_DB_USER,
        password=C_DB_PASSWORD,
        host=C_DB_HOST,
        port=parse_port(C_DB_PORT),
        database=C_DB_NAME if use_database else None
    )

def ensure_db():
    url = create_url(use_database=False)

    engine = create_engine(url)
    conn = engine.connect()

    result = conn.execute(
        text(
            f"SELECT 1 FROM pg_database WHERE datname = :dbname"
        ),
        { 'dbname': C_DB_NAME }
    )

    if result.rowcount == 0:
        conn.execute(text("COMMIT"))
        conn.execute(
            text(f"CREATE DATABASE {C_DB_NAME}"),
        )
        print("Database creation: Done")
    else:
        print("Database creation: Database already exists - Skipping")

    conn.close()

engine = create_engine(create_url())
needs_create_table = False

def table_exists():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = :tablename"),
            {'tablename': C_TABLE_NAME}
        )

        return result.rowcount > 0

def delete_table():
    if not table_exists():
        print("Skip")
        return

    stmt = "DROP TABLE {}".format(C_TABLE_NAME)

    with engine.begin() as conn:
        print(stmt)
        conn.execute(DDL(stmt))


def init_import():
    pass

if __name__ == "__main__":
    # delete_all = click.confirm("Delete all data in table?", default=False)
    # import_all = click.confirm("Import all data?", default=False)

    # command = (delete_all, import_all)
    command = (True, True)

    if command == (True, True):
        # Completely delete table
        delete_table()
        needs_create_table = True
        init_import()
    elif command == (True, False):
        print(2)
    elif command == (False, True):
        print(3)
    elif command == (False, False):
        print(4)
