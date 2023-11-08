from sqlalchemy import create_engine, text
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

def parse_port(port: str):
    try:
        num = int(port)
        return num
    except:
        return 5432

def ensure_db():
    url = URL.create(
        drivername="postgresql",
        username=C_DB_USER,
        password=C_DB_PASSWORD,
        host=C_DB_HOST,
        port=parse_port(C_DB_PORT)
    )

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

ensure_db()