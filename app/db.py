import os
import psycopg2


def get_conn():
    host = os.getenv("DB_HOST", "localhost")
    dbname = os.getenv("DB_NAME", "renaultdb")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASS")  # no default on purpose
    port = int(os.getenv("DB_PORT", "5432"))

    if password is None or password == "":
        raise RuntimeError(
            "DB_PASS is not set. Set DB_HOST/DB_NAME/DB_USER/DB_PASS (and optional DB_PORT) "
            "before starting FastAPI."
        )

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )
