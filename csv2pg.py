#!/usr/bin/env python

import csv
import logging
import argparse
from pathlib import Path

import click
from tqdm import tqdm
from sqlalchemy import create_engine, Engine, text, DDL
from sqlalchemy.engine import URL

from configparser import ConfigParser

class GlobalConfig:
    C_CONFIGDIR: Path
    C_DB_NAME: str
    C_DB_USER: str
    C_DB_PASSWORD: str
    C_DB_HOST: str
    C_DB_PORT: str
    C_TABLE_NAME: str
    C_CSV_DIRECTORY: str
    C_DEBUG: bool

g = GlobalConfig()

def init_config(config_file_loc: str, debug: bool):
    config = ConfigParser()
    with open(config_file_loc) as stream:
        # Append a fake section
        config.read_string("[top]\n" + stream.read())

    config = config['top']

    configdir = Path(config_file_loc).parent.resolve()

    g.C_CONFIGDIR = configdir
    g.C_DB_NAME = config.get('db_name', None)
    g.C_DB_USER = config.get('db_user', None)
    g.C_DB_PASSWORD = config.get('db_password', None)
    g.C_DB_HOST = config.get('db_host', None)
    g.C_DB_PORT = config.get('db_port', None)
    g.C_TABLE_NAME = config.get('table_name', None)
    g.C_CSV_DIRECTORY = config.get('csv_directory', None)
    g.C_DEBUG = bool(debug)

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
        username=g.C_DB_USER,
        password=g.C_DB_PASSWORD,
        host=g.C_DB_HOST,
        port=parse_port(g.C_DB_PORT),
        database=g.C_DB_NAME if use_database else None
    )

def ensure_db():
    url = create_url(use_database=False)

    engine = create_engine(url)
    conn = engine.connect()

    result = conn.execute(
        text(
            f"SELECT 1 FROM pg_database WHERE datname = :dbname"
        ),
        { 'dbname': g.C_DB_NAME }
    )

    if result.rowcount == 0:
        conn.execute(text("COMMIT"))
        conn.execute(
            text(f"CREATE DATABASE {g.C_DB_NAME}"),
        )
        print("Database creation: Done")
    else:
        print("Database creation: Database already exists - Skipping")

    conn.close()

engine: Engine

def table_exists():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = :tablename"),
            {'tablename': g.C_TABLE_NAME}
        )

        return result.rowcount > 0

def delete_table():
    if not table_exists():
        return

    stmt = "DROP TABLE {}".format(g.C_TABLE_NAME)

    with engine.begin() as conn:
        # print(stmt)
        conn.execute(DDL(stmt))

def create_table(csv_columns):
    columns = []
    with open(g.C_CONFIGDIR / 'columns.txt') as f:
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

    stmt = "CREATE TABLE {} ({});".format(g.C_TABLE_NAME, syn)

    with engine.begin() as conn:
        conn.execute(DDL(stmt))

    return len(columns)


def init_import(needs_create_table: bool):
    folder = Path(g.C_CSV_DIRECTORY)
    files = sorted(folder.glob('*.csv'))

    if len(files) == 0:
        print("No CSV found")
        return

    awaiting_columns = needs_create_table

    active_chunk = []
    max_chunk_len = 50

    count = 0

    def send_chunk():
        if len(active_chunk) == 0:
            return

        col_count = len(active_chunk[0])

        placeholder_values = ",".join([f":v{i}" for i in range(col_count)])
        stmt = "INSERT INTO {} VALUES ({})".format(g.C_TABLE_NAME, placeholder_values)
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

    pbar_rows = tqdm(desc="Processing rows")
    pbar_files = tqdm(total=len(files), desc="Files read", colour="#E36576")

    for file in files:
        logging.info("Reading file {}".format(file))
        pbar_rows.set_description("Processing rows from file \"{}\"".format(str(file)))
        with open(file, newline='') as f:
            reader = csv.reader(f)
            pbar_rows.update()
            for i, row in enumerate(reader):
                if i == 0:
                    if awaiting_columns:
                        awaiting_columns = False
                        create_table(row)
                    continue

                active_chunk.append(row)
                if len(active_chunk) >= max_chunk_len:
                    count += len(active_chunk)
                    send_chunk()

            count += len(active_chunk)
            send_chunk()
        pbar_files.update()

    pbar_rows.close()
    pbar_files.close()

    count += len(active_chunk)
    send_chunk()

    logging.info("{} row(s) added".format(count))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import data from CSV files to PostgreSQL')

    parser.add_argument('config_file', action='store', type=str, help="location of config.txt file")
    parser.add_argument('--debug', action='store_true', help="Enable debug mode")
    parser.add_argument('--update', action='store_true', help="Update existing records")

    args = parser.parse_args()

    init_config(args.config_file, debug=args.debug)

    if g.C_DEBUG:
        logging.basicConfig(
            filename="debug.log",
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG
        )

    engine = create_engine(create_url())

    if not args.update:
        delete_all = click.confirm("Delete all data in table?", default=False)
        import_all = click.confirm("Import all data?", default=False)

        command = (delete_all, import_all)
    else:
        command = (True, True)


    if command == (True, True):
        logging.info("Deleting table {}".format(g.C_TABLE_NAME))
        delete_table()
        print("Table deleted")

        logging.info("Importing data")
        init_import(needs_create_table=True)
        print("Import complete")
    elif command == (True, False):
        logging.info("Deleting table {}".format(g.C_TABLE_NAME))
        delete_table()
        print("Table deleted")
    elif command == (False, True):
        create_tbl = not table_exists()
        logging.info("Importing data".format(g.C_TABLE_NAME))
        init_import(needs_create_table=create_tbl)
        print("Import complete")
    elif command == (False, False):
        print("Quitting")

