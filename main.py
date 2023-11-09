import click
import csv
from pathlib import Path

from sqlalchemy import create_engine, Engine, text, DDL
from sqlalchemy.engine import URL

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
        # print(stmt)
        conn.execute(DDL(stmt))

def create_table(csv_columns):
    columns = []
    with open('columns.txt') as f:
        for line in f:
            parts = line.strip().split(" ")
            name = parts[0]
            datatype = parts[1]
            columns.append((name, datatype))

    # mappings = {}
    # if Path('mapcolumns.txt').is_file():
    #     with open('mapcolumns.txt') as f:
    #         for line in f:
    #             line = line.strip().split(" ")
    #             map_from = line[0] 
    #             map_to = line[1] 
    #             mappings[map_from] = map_to

    syn = ",\n".join([f"\"{col[0]}\" {col[1]}" for col in columns])

    stmt = "CREATE TABLE {} ({});".format(C_TABLE_NAME, syn)
    # print(stmt)

    with engine.begin() as conn:
        conn.execute(DDL(stmt))

    return len(columns)


def init_import(needs_create_table: bool):
    folder = Path('./csvs')
    files = sorted(folder.glob('*.csv'))

    if len(files) == 0:
        print("No CSV found")
        return

    awaiting_columns = needs_create_table

    active_chunk = []
    max_chunk_len = 5

    def send_chunk():
        if len(active_chunk) == 0:
            return

        col_count = len(active_chunk[0])

        placeholder_values = ",".join([f":v{i}" for i in range(col_count)])
        stmt = "INSERT INTO {} VALUES ({})".format(C_TABLE_NAME, placeholder_values)
        values = [
            {
                f"v{i}": val
                for i, val in enumerate(row)
            }
            for row in active_chunk
        ]

        with engine.begin() as conn:
            conn.execute(text(stmt), values)

        active_chunk.clear()

    for file in files:
        with open(file, newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    if awaiting_columns:
                        awaiting_columns = False
                        create_table(row)
                    continue

                active_chunk.append(row)
                if len(active_chunk) >= max_chunk_len:
                    send_chunk()

            send_chunk()

    send_chunk()


if __name__ == "__main__":
    delete_all = click.confirm("Delete all data in table?", default=False)
    import_all = click.confirm("Import all data?", default=False)

    command = (delete_all, import_all)
    # command = (True, True)

    if command == (True, True):
        # Completely delete table
        delete_table()
        init_import(needs_create_table=True)
    elif command == (True, False):
        delete_table()
    elif command == (False, True):
        create_tbl = not table_exists()
        init_import(needs_create_table=create_tbl)
    elif command == (False, False):
        print("Quitting")
