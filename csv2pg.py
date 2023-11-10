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

def get_col_mappings():
    mappings = {}
    map_file = Path(g.C_CONFIGDIR / 'mapcolumns.txt')
    if map_file.is_file():
        with map_file.open() as f:
            for line in f:
                parts = line.split(">")
                map_from = parts[0].strip()
                map_to = parts[1].strip()
                mappings[map_from] = map_to

    return mappings

def diff_additions(old, new):
    s_old = set(old)
    s_new = set(new)

    return list(s_new.difference(s_old))

def infer_cols_from_db():
    if not table_exists():
        return None

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT column_name FROM information_schema.columns where table_name = '{}';".format(g.C_TABLE_NAME)
        ))
        all_rows = result.all()
        cols = [row[0] for row in all_rows]
        return cols


def update_columns_to(prev_cols, new_cols):
    if prev_cols is None:
        # Create a new table
        syn = ",\n".join([f"\"{col}\" TEXT" for col in new_cols])
        stmt = "CREATE TABLE {} ({});".format(g.C_TABLE_NAME, syn)
        with engine.begin() as conn:
            conn.execute(DDL(stmt))

        return new_cols

    added_cols = diff_additions(prev_cols, new_cols)
    if len(added_cols) == 0:
        return prev_cols

    syn = ",\n".join([f"ADD COLUMN \"{col}\" TEXT" for col in added_cols])
    stmt = "ALTER TABLE {} {};".format(g.C_TABLE_NAME, syn)

    with engine.begin() as conn:
        conn.execute(DDL(stmt))

    return [*prev_cols, *added_cols]

def import_file(reader, csv_cols):
    active_chunk = []
    max_chunk_len = 50

    count = 0

    def send_chunk():
        active_chunk.clear()

    for row in reader:
        active_chunk.append(row)
        if len(active_chunk) >= max_chunk_len:
            count += len(active_chunk)
            send_chunk()

    count += len(active_chunk)
    send_chunk()

    return count

def process_filelist(files):
    pbar_files = tqdm(total=len(files), desc="Files read", colour="#E36576")

    prev_cols = infer_cols_from_db()
    col_mappings = get_col_mappings()

    total_rows = 0

    for filepath in files:
        logging.info("Reading file {}".format(filepath))

        file = filepath.open()
        reader = csv.reader(file)
        csv_cols = [
            col_mappings.get(col, col)
            for col in next(reader) 
        ]

        prev_cols = update_columns_to(prev_cols, csv_cols)
        total_rows += import_file(reader, csv_cols)
        pbar_files.update()

    pbar_files.close()
    logging.info("{} row(s) added".format(total_rows))


def init_import():
    folder = Path(g.C_CSV_DIRECTORY)
    files = sorted(folder.glob('*.csv'))

    if len(files) == 0:
        print("No CSV found")
        return

    process_filelist(files)


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

    ensure_db()
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
        init_import()
        print("Import complete")
    elif command == (True, False):
        logging.info("Deleting table {}".format(g.C_TABLE_NAME))
        delete_table()
        print("Table deleted")
    elif command == (False, True):
        logging.info("Importing data".format(g.C_TABLE_NAME))
        init_import()
        print("Import complete")
    elif command == (False, False):
        print("Quitting")

